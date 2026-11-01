package orchestrator

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"marc-runtime/src/internal/app"
	"marc-runtime/src/internal/statestore"
)

func protectedWorktreeFixture(t *testing.T, defaultDir bool) (context.Context, *statestore.StateStore, *Runtime, string, string) {
	t.Helper()
	repo := t.TempDir()
	if err := os.Chmod(repo, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		t.Helper()
		if raw, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git: %v %s", err, raw)
		}
	}
	git("init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "task.txt"), []byte("before\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "task.txt")
	git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
	data := filepath.Join(repo, "state[1]*?")
	if defaultDir {
		t.Setenv("XDG_DATA_HOME", filepath.Join(repo, "home"))
		data = ""
	}
	ctx := statestore.WithMutationActor(context.Background(), "ORCHESTRATOR")
	s, err := app.Start(ctx, data, statestore.OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	r, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, s, r, repo, filepath.Dir(s.Info().Path)
}

// Observe actual opens, including any done by Git enumeration. Metadata checks
// and inotify watches do not open/close SQLite-managed files.
func watchProtectedOpens(t *testing.T, dir string) func() {
	t.Helper()
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unix.InotifyAddWatch(fd, dir, unix.IN_OPEN); err != nil {
		unix.Close(fd)
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Close(fd) })
	return func() {
		t.Helper()
		var raw [4096]byte
		n, err := unix.Read(fd, raw[:])
		if n > 0 || !errors.Is(err, unix.EAGAIN) {
			t.Fatalf("protected directory or child was opened: events=%d err=%v", n, err)
		}
	}
}

func TestProtectedWorktreeCapture(t *testing.T) {
	for _, defaultDir := range []bool{false, true} {
		name := "custom"
		if defaultDir {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			ctx, _, r, repo, data := protectedWorktreeFixture(t, defaultDir)
			private := filepath.Join(data, "private.txt")
			if err := os.WriteFile(private, []byte("private sentinel"), 0600); err != nil {
				t.Fatal(err)
			}
			// Tracked protected content must also be excluded, independently of
			// ignore rules and of filename extensions/pattern metacharacters.
			if raw, err := exec.Command("git", "-C", repo, "add", "-f", private).CombinedOutput(); err != nil {
				t.Fatal(err, string(raw))
			}
			if err := os.Symlink(filepath.Join(data, statestore.DatabaseName), filepath.Join(data, ".gitignore")); err != nil {
				t.Fatal(err)
			}
			if err := unix.Mkfifo(filepath.Join(data, "opaque"), 0600); err != nil {
				t.Fatal(err)
			}
			for link, target := range map[string]string{
				"private-alias":    private,
				"directory-alias":  data,
				"alias-chain":      "directory-alias/private.txt",
				"dangling-private": filepath.Join(data, "not-created"),
			} {
				if err := os.Symlink(target, filepath.Join(repo, link)); err != nil {
					t.Fatal(err)
				}
			}
			bridge := filepath.Join(t.TempDir(), "bridge")
			if err := os.Symlink(data, bridge); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(bridge, filepath.Join(repo, "outside-alias")); err != nil {
				t.Fatal(err)
			}
			sibling := data + "-source"
			if err := os.Mkdir(sibling, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(sibling, "visible.txt"), []byte("source"), 0600); err != nil {
				t.Fatal(err)
			}
			for _, version := range []int{0, 1} {
				assertNoOpens := watchProtectedOpens(t, data)
				snapshot, err := r.captureWorktree(ctx, repo, version)
				if err != nil {
					t.Fatal(err)
				}
				var names []string
				for _, f := range snapshot.Files {
					names = append(names, f.Path)
				}
				siblingRel, _ := filepath.Rel(repo, filepath.Join(sibling, "visible.txt"))
				if len(names) != 2 || !strings.Contains(strings.Join(names, "\n"), siblingRel) || !strings.Contains(strings.Join(names, "\n"), "task.txt") {
					t.Fatalf("protected/ordinary paths misclassified: %v", names)
				}
				assertNoOpens()
				if err := os.WriteFile(private, []byte("changed private sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
				assertNoOpens = watchProtectedOpens(t, data)
				again, err := r.captureWorktree(ctx, repo, version)
				if err != nil || !reflect.DeepEqual(snapshot, again) {
					t.Fatal("protected content changed capture identity", err)
				}
				assertNoOpens()
			}
		})
	}
}

func TestProtectedWorktreeRootsAndUnrelatedSymlinks(t *testing.T) {
	ctx, _, r, repo, data := protectedWorktreeFixture(t, false)
	alias := filepath.Join(repo, "alias")
	if err := os.Symlink(data, alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(data, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	assertNoOpens := watchProtectedOpens(t, data)
	for _, path := range []string{data, alias, filepath.Join(data, "nested"), filepath.Join(data, statestore.DatabaseName), filepath.Join(alias, statestore.DatabaseName)} {
		if _, err := r.snapshotWorktree(ctx, path); err == nil || !strings.Contains(err.Error(), "protected") || strings.Contains(err.Error(), data) {
			t.Fatalf("protected root/file rejection: %s: %v", path, err)
		}
	}
	assertNoOpens()
	for _, target := range []string{"task.txt", "missing", "ordinary-link"} {
		link := filepath.Join(repo, "ordinary-link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := r.snapshotWorktree(ctx, repo); err == nil {
			t.Fatal("unrelated symlink behavior changed")
		}
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProtectedWorktreeRenamedDataDirectory(t *testing.T) {
	ctx, _, r, repo, data := protectedWorktreeFixture(t, false)
	moved := filepath.Join(repo, "renamed-vault")
	if err := os.Rename(data, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "replacement.txt"), []byte("also protected"), 0600); err != nil {
		t.Fatal(err)
	}
	assertNoOpens := watchProtectedOpens(t, moved)
	snapshot, err := r.snapshotWorktree(ctx, repo)
	if err != nil || len(snapshot.Files) != 1 || snapshot.Files[0].Path != "task.txt" {
		t.Fatal("held/configured data directory not protected after rename", snapshot, err)
	}
	assertNoOpens()
}

func TestProtectedWorktreeSubstitutionBeforeOpen(t *testing.T) {
	ctx, _, r, repo, data := protectedWorktreeFixture(t, false)
	assertNoOpens := watchProtectedOpens(t, data)
	r.hook = func(cut string) error {
		if cut == "worktree_before_open" {
			if err := os.Remove(filepath.Join(repo, "task.txt")); err != nil {
				return err
			}
			return os.Symlink(filepath.Join(data, statestore.DatabaseName), filepath.Join(repo, "task.txt"))
		}
		return nil
	}
	if _, err := r.snapshotWorktree(ctx, repo); err == nil {
		t.Fatal("substituted symlink followed")
	}
	assertNoOpens()
}

func TestProtectedWorktreeDirectorySubstitution(t *testing.T) {
	ctx, _, r, repo, data := protectedWorktreeFixture(t, false)
	source := filepath.Join(repo, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	assertNoOpens := watchProtectedOpens(t, data)
	swapped := false
	r.hook = func(cut string) error {
		if cut == "worktree_before_open" && !swapped {
			swapped = true
			if err := os.Rename(source, filepath.Join(repo, "old-source")); err != nil {
				return err
			}
			return os.Rename(data, source)
		}
		return nil
	}
	snapshot, err := r.snapshotWorktree(ctx, repo)
	if err != nil || len(snapshot.Files) != 1 || snapshot.Files[0].Path != "task.txt" {
		t.Fatal("substituted protected directory was captured", snapshot, err)
	}
	assertNoOpens()
}

func TestProtectedWorktreeGitMetadataOverlap(t *testing.T) {
	ctx, _, _, repo, _ := protectedWorktreeFixture(t, false)
	data := filepath.Join(repo, ".git", "private")
	s, err := app.Start(ctx, data, statestore.OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	assertNoOpens := watchProtectedOpens(t, data)
	if _, err := r.snapshotWorktree(ctx, repo); err == nil || !strings.Contains(err.Error(), "Git metadata") {
		t.Fatal("protected Git metadata overlap did not fail closed", err)
	}
	assertNoOpens()
}

func TestProtectedWorktreeRootSubstitution(t *testing.T) {
	ctx, _, r, repo, data := protectedWorktreeFixture(t, false)
	assertNoOpens := watchProtectedOpens(t, data)
	moved := filepath.Join(t.TempDir(), "moved")
	r.hook = func(cut string) error {
		if cut == "worktree_before_open" {
			return os.Rename(repo, moved)
		}
		return nil
	}
	if _, err := r.snapshotWorktree(ctx, repo); err == nil || !strings.Contains(err.Error(), "location changed") {
		t.Fatal("renamed worktree root was not rejected", err)
	}
	assertNoOpens()
}

func TestProtectedWorktreeGitCommonDirectoryOverlap(t *testing.T) {
	for _, layout := range []string{"directory", "gitfile", "symlink"} {
		t.Run(layout, func(t *testing.T) {
			ctx, _, r, repo, data := protectedWorktreeFixture(t, false)
			// Git honors commondir even when .git is a directory. Populate
			// only this disposable data fixture with common Git metadata.
			if raw, err := exec.Command("git", "init", "--bare", "-q", data).CombinedOutput(); err != nil {
				t.Fatal(err, string(raw))
			}
			git := filepath.Join(repo, ".git")
			if layout != "directory" {
				moved := filepath.Join(t.TempDir(), "metadata")
				if err := os.Rename(git, moved); err != nil {
					t.Fatal(err)
				}
				var err error
				if layout == "gitfile" {
					err = os.WriteFile(git, []byte("gitdir: "+moved+"\n"), 0600)
				} else {
					err = os.Symlink(moved, git)
				}
				if err != nil {
					t.Fatal(err)
				}
				git = moved
			}
			if err := os.WriteFile(filepath.Join(git, "commondir"), []byte(data+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			assertNoOpens := watchProtectedOpens(t, data)
			_, err := r.snapshotWorktree(ctx, repo)
			assertNoOpens()
			if err == nil || !strings.Contains(err.Error(), "Git metadata") {
				t.Fatal("protected Git common directory did not fail closed", err)
			}
		})
	}
}
