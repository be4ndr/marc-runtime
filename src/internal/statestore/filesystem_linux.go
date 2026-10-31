package statestore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// Held descriptors prevent component substitution. Ancestors forbid group/other
// writers (sticky temporary roots excepted). The host's mount namespace is trusted.
func openLocalDirectory(path string, create, private bool) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || isWindowsMount(path) {
		return nil, fmt.Errorf("directory requires an absolute canonical local path")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if fd >= 0 {
			unix.Close(fd)
		}
	}()
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		var parent unix.Stat_t
		if err := unix.Fstat(fd, &parent); err != nil {
			return nil, err
		}
		if parent.Mode&0022 != 0 && parent.Mode&unix.S_ISVTX == 0 {
			return nil, fmt.Errorf("directory has an untrusted writable ancestor")
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err == unix.ENOENT && create {
			var fs unix.Statfs_t
			if err := unix.Fstatfs(fd, &fs); err != nil {
				return nil, err
			}
			if err := validateFilesystemType(fs.Type, true); err != nil {
				return nil, err
			}
			if err = unix.Mkdirat(fd, part, 0700); err != nil && err != unix.EEXIST {
				return nil, err
			}
			next, err = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		}
		if err != nil {
			return nil, err
		}
		unix.Close(fd)
		fd = next
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, err
	}
	if st.Mode&0022 != 0 && st.Mode&unix.S_ISVTX == 0 {
		return nil, fmt.Errorf("directory has an untrusted writable parent")
	}
	if private && !privateDirectory(st) {
		return nil, fmt.Errorf("directory must be controller-owned mode 0700 (owner=%d, mode=%#o)", st.Uid, st.Mode&0777)
	}
	f := os.NewFile(uintptr(fd), path)
	fd = -1
	return f, nil
}

func privateDirectory(st unix.Stat_t) bool {
	return st.Uid == uint32(os.Geteuid()) && st.Mode&07777 == 0700
}

func descriptorPath(dir *os.File) string { return fmt.Sprintf("/proc/self/fd/%d", dir.Fd()) }

// Projection directories share the same local/private path policy, but do not
// acquire authoritative database lifetime or durability semantics.
func prepareDataDir(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("data directory is required")
	}
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == ".." {
			return "", fmt.Errorf("parent traversal is not a data directory")
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	dir, err := openLocalDirectory(abs, true, true)
	if err != nil {
		return "", err
	}
	defer dir.Close()
	return abs, nil
}

func openLocalRegularFile(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("file requires an absolute canonical path")
	}
	dir, err := openLocalDirectory(filepath.Dir(path), false, false)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	fd, err := unix.Openat(int(dir.Fd()), filepath.Base(path), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("file must be regular")
	}
	var fs unix.Statfs_t
	if err = unix.Fstatfs(fd, &fs); err == nil {
		err = validateFilesystemType(fs.Type, true)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
