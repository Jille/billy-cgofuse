// Package billycgofuse exposes a cgofuse.FileSystemInterface that passes calls to a Billy API.
package billycgofuse

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/go-git/go-billy/v5"
)

func New(underlying billy.Basic) fuse.FileSystemInterface {
	return &wrapper{
		underlying:      underlying,
		fileDescriptors: map[uint64]billy.File{},
		writeLocks:      map[uint64]*sync.Mutex{},
	}
}

type wrapper struct {
	fuse.FileSystemBase
	underlying billy.Basic

	fdMtx           sync.Mutex
	fileDescriptors map[uint64]billy.File
	nextFd          uint64
	writeLocks      map[uint64]*sync.Mutex
}

// Init is called when the file system is created.
func (w *wrapper) Init() {
}

// Destroy is called when the file system is destroyed.
func (w *wrapper) Destroy() {
}

// Statfs gets file system statistics.
func (w *wrapper) Statfs(path string, stat *fuse.Statfs_t) int {
	return -fuse.ENOSYS
}

// Mknod creates a file node.
func (w *wrapper) Mknod(path string, mode uint32, dev uint64) int {
	return -fuse.ENOSYS
}

// Mkdir creates a directory.
func (w *wrapper) Mkdir(path string, mode uint32) int {
	if dfs, ok := w.underlying.(billy.Dir); ok {
		return convertError(dfs.MkdirAll(path, os.FileMode(mode)))
	}
	return -fuse.ENOSYS
}

// Unlink removes a file.
func (w *wrapper) Unlink(path string) int {
	return convertError(w.underlying.Remove(path))
}

// Rmdir removes a directory.
func (w *wrapper) Rmdir(path string) int {
	return convertError(w.underlying.Remove(path))
}

// Link creates a hard link to a file.
func (w *wrapper) Link(oldpath, newpath string) int {
	return -fuse.ENOSYS
}

// Symlink creates a symbolic link.
func (w *wrapper) Symlink(target, newpath string) int {
	if sfs, ok := w.underlying.(billy.Symlink); ok {
		return convertError(sfs.Symlink(target, newpath))
	}
	return -fuse.ENOSYS
}

// Readlink reads the target of a symbolic link.
func (w *wrapper) Readlink(path string) (int, string) {
	if sfs, ok := w.underlying.(billy.Symlink); ok {
		fn, err := sfs.Readlink(path)
		if err != nil {
			return convertError(err), ""
		}
		return 0, fn
	}
	return -fuse.ENOSYS, ""
}

// Rename renames a file.
func (w *wrapper) Rename(oldpath, newpath string) int {
	return convertError(w.underlying.Rename(oldpath, newpath))
}

// Chmod changes the permission bits of a file.
func (w *wrapper) Chmod(path string, mode uint32) int {
	if cfs, ok := w.underlying.(billy.Change); ok {
		return convertError(cfs.Chmod(path, os.FileMode(mode)))
	}
	return -fuse.ENOSYS
}

// Chown changes the owner and group of a file.
func (w *wrapper) Chown(path string, uid uint32, gid uint32) int {
	if cfs, ok := w.underlying.(billy.Change); ok {
		return convertError(cfs.Chown(path, int(uid), int(gid)))
	}
	return -fuse.ENOSYS
}

// Utimens changes the access and modification times of a file.
func (w *wrapper) Utimens(path string, tmsp []fuse.Timespec) int {
	if cfs, ok := w.underlying.(billy.Change); ok {
		if len(tmsp) != 2 {
			return -fuse.EINVAL
		}
		return convertError(cfs.Chtimes(path, tmsp[0].Time(), tmsp[1].Time()))
	}
	return -fuse.ENOSYS
}

// Access checks file access permissions.
func (w *wrapper) Access(path string, mask uint32) int {
	return -fuse.ENOSYS
}

func (w *wrapper) createFileDescriptor(fh billy.File) uint64 {
	w.fdMtx.Lock()
	defer w.fdMtx.Unlock()
	w.nextFd++
	fd := w.nextFd
	w.fileDescriptors[fd] = fh
	w.writeLocks[fd] = new(sync.Mutex)
	return fd
}

func (w *wrapper) getFileDescriptor(fd uint64) (billy.File, bool) {
	w.fdMtx.Lock()
	defer w.fdMtx.Unlock()
	fh, ok := w.fileDescriptors[fd]
	return fh, ok
}

func (w *wrapper) getFileDescriptorWithLock(fd uint64) (billy.File, func(), bool) {
	w.fdMtx.Lock()
	defer w.fdMtx.Unlock()
	fh, ok := w.fileDescriptors[fd]
	w.writeLocks[fd].Lock()
	unlock := w.writeLocks[fd].Unlock
	return fh, unlock, ok
}

// Create creates and opens a file.
// The flags are a combination of the fuse.O_* constants.
func (w *wrapper) Create(path string, flags int, mode uint32) (int, uint64) {
	fh, err := w.underlying.OpenFile(path, flags|os.O_CREATE|os.O_RDWR, os.FileMode(mode))
	if err != nil {
		return convertError(err), 0
	}
	return 0, w.createFileDescriptor(fh)
}

// Open opens a file.
// The flags are a combination of the fuse.O_* constants.
func (w *wrapper) Open(path string, flags int) (int, uint64) {
	fh, err := w.underlying.OpenFile(path, flags|os.O_RDONLY, 0777)
	if err != nil {
		return convertError(err), 0
	}
	return 0, w.createFileDescriptor(fh)
}

// Getattr gets file attributes.
// Note that Billy doesn't support Stat on a filedescriptor, so we ignore the fd.
func (w *wrapper) Getattr(path string, stat *fuse.Stat_t, fd uint64) int {
	fi, err := w.underlying.Stat(path)
	if err != nil {
		return convertError(err)
	}
	fileInfoToStat(fi, stat)
	return 0
}

// Truncate changes the size of a file.
func (w *wrapper) Truncate(path string, size int64, fd uint64) int {
	if fd != ^uint64(0) {
		fh, ok := w.getFileDescriptor(fd)
		if !ok {
			return -fuse.EINVAL
		}
		return convertError(fh.Truncate(size))
	}
	// Billy doesn't support Truncate on a path.
	fh, err := w.underlying.OpenFile(path, os.O_WRONLY, 0777)
	if err != nil {
		return convertError(err)
	}
	defer fh.Close()
	return convertError(fh.Truncate(size))
}

// Read reads data from a file.
func (w *wrapper) Read(path string, buff []byte, ofst int64, fd uint64) int {
	fh, ok := w.getFileDescriptor(fd)
	if !ok {
		return -fuse.EINVAL
	}
	n, err := fh.ReadAt(buff, ofst)
	if n > 0 || err == io.EOF {
		return n
	}
	return convertError(err)
}

// Write writes data to a file.
func (w *wrapper) Write(path string, buff []byte, ofst int64, fd uint64) int {
	fh, unlock, ok := w.getFileDescriptorWithLock(fd)
	if !ok {
		return -fuse.EINVAL
	}
	if wa, ok := fh.(io.WriterAt); ok {
		unlock()
		n, err := wa.WriteAt(buff, ofst)
		if err != nil {
			return convertError(err)
		}
		return n
	}
	defer unlock()
	if _, err := fh.Seek(ofst, io.SeekStart); err != nil {
		return convertError(err)
	}
	n, err := fh.Write(buff)
	if err != nil {
		return convertError(err)
	}
	return n
}

// Flush flushes cached file data.
func (w *wrapper) Flush(path string, fd uint64) int {
	return -fuse.ENOSYS
}

// Release closes an open file.
func (w *wrapper) Release(path string, fd uint64) int {
	w.fdMtx.Lock()
	defer w.fdMtx.Unlock()
	fh, ok := w.fileDescriptors[fd]
	if !ok {
		return -fuse.EINVAL
	}
	delete(w.fileDescriptors, fd)
	// It's fine if the write lock is still being held. The Close will soon unblock that.
	delete(w.writeLocks, fd)
	return convertError(fh.Close())
}

// Fsync synchronizes file contents.
func (w *wrapper) Fsync(path string, datasync bool, fd uint64) int {
	return -fuse.ENOSYS
}

// Opendir opens a directory.
func (w *wrapper) Opendir(path string) (int, uint64) {
	w.fdMtx.Lock()
	defer w.fdMtx.Unlock()
	w.nextFd++
	return 0, w.nextFd
}

func fileInfoToStat(fi os.FileInfo, out *fuse.Stat_t) {
	*out = fuse.Stat_t{
		Size: fi.Size(),
		Mtim: fuse.NewTimespec(fi.ModTime()),
		Mode: uint32(fi.Mode()),
	}
	if fi.IsDir() {
		out.Mode |= fuse.S_IFDIR
	}
}

// Readdir reads a directory.
// Note that Billy doesn't support ReadDir on a filedescriptor, so we ignore the fd.
func (w *wrapper) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) int {
	if dfs, ok := w.underlying.(billy.Dir); ok {
		entries, err := dfs.ReadDir(path)
		if err != nil {
			return convertError(err)
		}
		for _, e := range entries {
			st := new(fuse.Stat_t)
			fileInfoToStat(e, st)
			fill(e.Name(), st, 0)
		}
	}
	return -fuse.ENOSYS
}

// Releasedir closes an open directory.
func (w *wrapper) Releasedir(path string, fd uint64) int {
	return 0
}

// Fsyncdir synchronizes directory contents.
func (w *wrapper) Fsyncdir(path string, datasync bool, fd uint64) int {
	return -fuse.ENOSYS
}

// Setxattr sets extended attributes.
func (w *wrapper) Setxattr(path string, name string, value []byte, flags int) int {
	return -fuse.ENOSYS
}

// Getxattr gets extended attributes.
func (w *wrapper) Getxattr(path string, name string) (int, []byte) {
	return -fuse.ENOSYS, nil
}

// Removexattr removes extended attributes.
func (w *wrapper) Removexattr(path string, name string) int {
	return -fuse.ENOSYS
}

// Listxattr lists extended attributes.
func (w *wrapper) Listxattr(path string, fill func(name string) bool) int {
	return -fuse.ENOSYS
}

func convertError(err error) int {
	if err == nil {
		return 0
	}
	if os.IsExist(err) {
		return -fuse.EEXIST
	}
	if os.IsNotExist(err) {
		return -fuse.ENOENT
	}
	if os.IsPermission(err) {
		return -fuse.EPERM
	}
	if errors.Is(err, os.ErrInvalid) || errors.Is(err, os.ErrClosed) {
		return -fuse.EINVAL
	}
	return -fuse.EIO
}
