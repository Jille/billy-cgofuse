# billy-cgofuse

[![GoDoc](https://godoc.org/github.com/Jille/billy-cgofuse?status.svg)](https://godoc.org/github.com/Jille/billy-cgofuse)

Each Go fuse library has its own interface that it expects from users. Billy is a standard interface for filesystems.

This library receives calls from cgofuse and sends them to a billy.Filesystem, allowing for easily swapping out both sides.

If you need additional methods not exposed by Billy, you can always embed the fuse.FileSystemInterface we create in your own struct and override methods at will.
