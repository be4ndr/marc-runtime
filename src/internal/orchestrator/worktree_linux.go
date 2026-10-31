package orchestrator

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// This boundary belongs to one capture, not an authoritative state cache. Both
// the configured name and the held directory's current identity stay protected.
type worktreeProtection struct {
	paths []string
	info  os.FileInfo
}

func pathWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, "../")
}

func (r *Runtime) worktreeProtection() (worktreeProtection, error) {
	path, info, err := r.store.ProtectedDataDirectory()
	if err != nil {
		return worktreeProtection{}, err
	}
	return worktreeProtection{[]string{filepath.Dir(r.store.Info().Path), path}, info}, nil
}

func (p worktreeProtection) lexical(path string) bool {
	for _, root := range p.paths {
		if pathWithin(root, path) {
			return true
		}
	}
	return false
}

// Resolve with metadata only, stopping at the protected boundary even for a
// dangling link into it. No SQLite-managed file is opened to classify a path.
func (p worktreeProtection) contains(path string) (bool, error) {
	links := 0
	for {
		if p.lexical(path) {
			return true, nil
		}
		parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
		current := "/"
		restart := false
		for i, part := range parts {
			next := filepath.Join(current, part)
			if p.lexical(next) {
				return true, nil
			}
			info, err := os.Lstat(next)
			if os.IsNotExist(err) {
				return false, nil
			}
			if err != nil {
				return false, fmt.Errorf("cannot establish worktree path safety")
			}
			if os.SameFile(p.info, info) {
				return true, nil
			}
			if info.Mode()&os.ModeSymlink != 0 {
				links++
				if links > 40 {
					return false, fmt.Errorf("cannot resolve worktree symlink safely")
				}
				target, err := os.Readlink(next)
				if err != nil {
					return false, fmt.Errorf("cannot resolve worktree symlink safely")
				}
				if !filepath.IsAbs(target) {
					target = current + "/" + target
				}
				// Do not clean unresolved components: link/.. must resolve the
				// link before applying .., rather than collapse lexically.
				path = target
				if i+1 < len(parts) {
					path += "/" + strings.Join(parts[i+1:], "/")
				}
				restart = true
				break
			}
			current = next
		}
		if !restart {
			return false, nil
		}
	}
}

// Git must prune untracked protected directories too: otherwise enumeration can
// read their .gitignore files before capture has a chance to filter the names.
func (p worktreeProtection) gitExcludes(root string) []string {
	var args []string
	var pathspecs []string
	escape := strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`)
	for _, path := range p.paths {
		if pathWithin(root, path) {
			rel, _ := filepath.Rel(root, path)
			args = append(args, "--exclude=/"+escape.Replace(rel)+"/")
			pathspecs = append(pathspecs, ":(top,exclude,literal)"+rel)
		}
	}
	return append(append(args, "--", "."), pathspecs...)
}

func (p worktreeProtection) sameDirectory(st unix.Stat_t) bool {
	info := p.info.Sys().(*syscall.Stat_t)
	return st.Mode&unix.S_IFMT == unix.S_IFDIR && st.Dev == uint64(info.Dev) && st.Ino == info.Ino
}

// Pin each parent and use O_NOFOLLOW on every open. A symlink substituted after
// classification cannot redirect a content read into protected storage. Nil
// means a protected or deleted entry, which contributes no capture/evidence.
func (p worktreeProtection) openFile(root *os.File, name string) (*os.File, error) {
	if name == "" || filepath.IsAbs(name) || filepath.Clean(name) != name || name == "." || name == ".." || strings.HasPrefix(name, "../") {
		return nil, fmt.Errorf("unsafe worktree path")
	}
	dirfd := int(root.Fd())
	owned := false
	defer func() {
		if owned {
			unix.Close(dirfd)
		}
	}()
	parts := strings.Split(name, "/")
	for i, part := range parts {
		var st unix.Stat_t
		if err := unix.Fstatat(dirfd, part, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			if errors.Is(err, unix.ENOENT) {
				return nil, nil
			}
			return nil, fmt.Errorf("cannot establish worktree entry safety")
		}
		if p.sameDirectory(st) {
			return nil, nil
		}
		last := i == len(parts)-1
		if st.Mode&unix.S_IFMT == unix.S_IFLNK {
			return nil, fmt.Errorf("unsupported symlink in worktree")
		}
		if last && st.Mode&unix.S_IFMT != unix.S_IFREG || !last && st.Mode&unix.S_IFMT != unix.S_IFDIR {
			return nil, fmt.Errorf("worktree contains nonregular content")
		}
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
		if !last {
			flags |= unix.O_DIRECTORY
		}
		fd, err := unix.Openat(dirfd, part, flags, 0)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				return nil, nil
			}
			return nil, fmt.Errorf("worktree entry changed or cannot be opened safely")
		}
		if last {
			return os.NewFile(uintptr(fd), name), nil
		}
		var opened unix.Stat_t
		err = unix.Fstat(fd, &opened)
		if err != nil || p.sameDirectory(opened) || opened.Dev != st.Dev || opened.Ino != st.Ino {
			unix.Close(fd)
			return nil, fmt.Errorf("worktree directory changed during capture")
		}
		if owned {
			unix.Close(dirfd)
		}
		dirfd, owned = fd, true
	}
	return nil, fmt.Errorf("unsafe worktree path")
}

// Git metadata is read before Git starts, so a data directory overlapping its
// administrative directories fails closed rather than allowing Git to read it.
func (p worktreeProtection) checkGitDirectory(path string) error {
	protected, err := p.contains(path)
	if err != nil || protected {
		return fmt.Errorf("Git metadata overlaps protected data directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("cannot establish Git metadata safety")
	}
	protected, err = p.contains(resolved)
	if err != nil || protected {
		return fmt.Errorf("Git metadata overlaps protected data directory")
	}
	for _, data := range p.paths {
		if pathWithin(resolved, data) {
			return fmt.Errorf("Git metadata overlaps protected data directory")
		}
	}
	return nil
}

func (p worktreeProtection) checkGitMetadata(root *os.File, path string) error {
	git := filepath.Join(path, ".git")
	if err := p.checkGitDirectory(git); err != nil {
		return err
	}
	info, err := os.Stat(git)
	if err != nil {
		return fmt.Errorf("cannot establish Git metadata safety")
	}
	if !info.IsDir() {
		f, err := p.openFile(root, ".git")
		if info, statErr := os.Lstat(git); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			// Git administrative links are not project content. Preserve their
			// existing behavior while pinning the resolved metadata path safely.
			resolved, resolveErr := filepath.EvalSymlinks(git)
			if resolveErr != nil {
				return fmt.Errorf("cannot establish Git metadata safety")
			}
			systemRoot, openErr := os.Open("/")
			if openErr != nil {
				return fmt.Errorf("cannot establish Git metadata safety")
			}
			f, err = p.openFile(systemRoot, strings.TrimPrefix(resolved, "/"))
			systemRoot.Close()
		}
		if err != nil || f == nil {
			return fmt.Errorf("cannot establish Git metadata safety")
		}
		raw, err := io.ReadAll(io.LimitReader(f, 4097))
		f.Close()
		if err != nil || len(raw) > 4096 || !strings.HasPrefix(string(raw), "gitdir: ") {
			return fmt.Errorf("cannot establish Git metadata safety")
		}
		git = strings.TrimSpace(strings.TrimPrefix(string(raw), "gitdir: "))
		if !filepath.IsAbs(git) {
			git = filepath.Join(path, git)
		}
		if err := p.checkGitDirectory(git); err != nil {
			return err
		}
	}
	// Git honors commondir in both directory and gitfile layouts.
	dir, err := os.Open(git)
	if err != nil {
		return fmt.Errorf("cannot establish Git metadata safety")
	}
	defer dir.Close()
	actual, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", dir.Fd()))
	if err != nil {
		return fmt.Errorf("cannot establish Git metadata safety")
	}
	if err := p.checkGitDirectory(actual); err != nil {
		return err
	}
	f, err := p.openFile(dir, "commondir")
	if err != nil {
		return err
	}
	if f == nil {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	f.Close()
	if err != nil || len(raw) > 4096 {
		return fmt.Errorf("cannot establish Git metadata safety")
	}
	common := strings.TrimSpace(string(raw))
	if !filepath.IsAbs(common) {
		common = filepath.Join(git, common)
	}
	return p.checkGitDirectory(common)
}
