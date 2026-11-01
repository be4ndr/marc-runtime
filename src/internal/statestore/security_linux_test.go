package statestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"marc-runtime/internal/identity"
	"marc-runtime/src/internal/roleresult"
)

func TestSecurityJSONRejectionIsAtomic(t *testing.T) {
	s, _ := accountingFixture(t)
	for i, raw := range []string{`{"x":"\ud800"}`, `{"x":1,"\u0078":2}`, `{"x":"` + strings.Repeat("x", 257) + `","x":"ok"}`, strings.Repeat("[", identity.MaxJSONDepth+1) + `0` + strings.Repeat("]", identity.MaxJSONDepth+1), `[` + strings.Repeat(`0,`, identity.MaxJSONTokens) + `0]`} {
		r := UsageReceipt{ReceiptID: fmt.Sprintf("unsafe-%d", i), RunID: "r1", InvocationID: "i1", ProviderCallID: "i1-call", Completeness: "unknown", ReportedAt: semanticTimestamp, ProviderRaw: json.RawMessage(raw)}
		if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); err == nil {
			t.Fatal("unsafe provider metadata accepted")
		}
		if resultCount(t, s, "usage_receipts") != 0 || resultCount(t, s, "events") != 0 {
			t.Fatal("rejection altered receipt/audit state")
		}
	}
	r := UsageReceipt{ReceiptID: "exact-metadata", RunID: "r1", InvocationID: "i1", ProviderCallID: "i1-call", Completeness: "unknown", ReportedAt: semanticTimestamp, ProviderRaw: json.RawMessage(`{"n":9007199254740993,"huge":1e1000000}`)}
	if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := s.db.QueryRow(`SELECT provider_raw_json FROM usage_receipts WHERE receipt_id=?`, r.ReceiptID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, "9007199254740993") || !strings.Contains(raw, "1e1000000") {
		t.Fatal("opaque numbers changed", raw)
	}
}

func TestSecurityWorkerResultAndTypedFindingRollback(t *testing.T) {
	s := testStore(t)
	raw := resultFixture(t, "reviewer-changes-required")
	r, err := roleresult.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	seedResultBinding(t, s, r, 1)
	f := newFinding(r)
	f.Summary = string([]byte{255})
	if err := s.AcceptRoleResult(orchestratorContext(), raw, []roleresult.Finding{f}, ResultFence{1, 0}); err == nil {
		t.Fatal("invalid typed finding accepted")
	}
	for _, bad := range [][]byte{bytes.Replace(raw, []byte(`"summary":`), []byte(`"unsafe":"\ud800","summary":`), 1), append(bytes.Repeat([]byte(" "), identity.MaxJSONBytes), raw...)} {
		if err := s.AcceptRoleResult(orchestratorContext(), bad, []roleresult.Finding{newFinding(r)}, ResultFence{1, 0}); err == nil {
			t.Fatal("unsafe result accepted")
		}
	}
	if resultCount(t, s, "role_results") != 0 || resultCount(t, s, "findings") != 0 || resultCount(t, s, "events") != 0 {
		t.Fatal("unsafe result changed authority/audit")
	}
	requireOwner(t, s, r.RunID, "REVIEWER", &r.InvocationID, 1, 0)
}

func TestSecurityPrivateDataRootAndNoSymlinks(t *testing.T) {
	if !privateDirectory(unix.Stat_t{Uid: uint32(os.Geteuid()), Mode: 0700}) || privateDirectory(unix.Stat_t{Uid: uint32(os.Geteuid() + 1), Mode: 0700}) || privateDirectory(unix.Stat_t{Uid: uint32(os.Geteuid()), Mode: 0770}) || privateDirectory(unix.Stat_t{Uid: uint32(os.Geteuid()), Mode: 01700}) {
		t.Fatal("private root owner/mode policy mismatch")
	}
	for _, mode := range []os.FileMode{0755, 0770, 0777} {
		dir := t.TempDir()
		if err := os.Chmod(dir, mode); err != nil {
			t.Fatal(err)
		}
		if s, err := Open(orchestratorContext(), dir); err == nil {
			s.Close()
			t.Fatal("nonprivate directory accepted")
		}
		if _, err := os.Lstat(filepath.Join(dir, DatabaseName)); !os.IsNotExist(err) {
			t.Fatal("rejected root created DB")
		}
	}
	root := t.TempDir()
	real := filepath.Join(root, "real")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(orchestratorContext(), filepath.Join(alias, "data")); err == nil {
		t.Fatal("symlink ancestor accepted")
	}
	if _, err := Open(orchestratorContext(), root+"/real/../data"); err == nil {
		t.Fatal("parent traversal accepted")
	}
	for _, kind := range []int64{0x01021994, 0x858458f6} {
		if validateFilesystemType(kind) == nil || validateFilesystemType(kind, true) != nil {
			t.Fatal("volatile storage opt-in not explicit")
		}
	}
	// Directory owner validation is applied to the held inode, not a pathname.
	dir, err := openLocalDirectory(real, false, true)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := os.Rename(real, filepath.Join(root, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), real); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(descriptorPath(dir), "anchored"), []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "moved", "anchored")); err != nil {
		t.Fatal("descriptor followed substituted pathname", err)
	}
}

func TestSecurityVolatileOpenRequiresExplicitTestOption(t *testing.T) {
	dir := privateTempDir(t)
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	s, err := Open(orchestratorContext(), dir)
	if fs.Type == 0x01021994 || fs.Type == 0x858458f6 {
		if err == nil {
			s.Close()
			t.Fatal("volatile production open accepted")
		}
		if _, err := os.Lstat(filepath.Join(dir, DatabaseName)); !os.IsNotExist(err) {
			t.Fatal("volatile rejection created DB")
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	s, err = Open(orchestratorContext(), dir, OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		t.Fatal("explicit volatile test fixture rejected", err)
	}
	s.Close()
}

func TestSecuritySQLiteDescriptorSurvivesRenameAndReconnect(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "db")
	s, err := Open(orchestratorContext(), dir, OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedInvocation(t, s)
	if err := os.Rename(dir, filepath.Join(root, "moved")); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.Symlink(target, dir); err != nil {
		t.Fatal(err)
	}
	s.db.SetMaxIdleConns(0)
	if resultCount(t, s, "invocations") != 1 {
		t.Fatal("replacement connection lost original DB")
	}
	if _, err := os.Stat(filepath.Join(target, DatabaseName)); !os.IsNotExist(err) {
		t.Fatal("SQLite opened substitute DB")
	}
}

func TestSecurityEvidenceBoundsAndAncestorProtection(t *testing.T) {
	s := testStore(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "evidence")
	raw := []byte("sealed")
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s.db, `INSERT INTO evidence_artifacts VALUES('bounded-file','test','file',?,?,NULL,?,'now')`, digest, len(raw), path)
	ref := ProofArtifact{"bounded-file", "test", digest}
	if err := verifyAuthorityArtifact(orchestratorContext(), s.db, ref); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 34); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := verifyAuthorityArtifact(orchestratorContext(), s.db, ref); err == nil {
		t.Fatal("oversized evidence accepted")
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if f, err := openAuthorityArtifact(filepath.Join(alias, "evidence")); err == nil {
		f.Close()
		t.Fatal("evidence symlink parent accepted")
	}
	fifo := filepath.Join(dir, "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	if f, err := openAuthorityArtifact(fifo); err == nil {
		f.Close()
		t.Fatal("FIFO evidence accepted")
	}
}

func TestSecurityDiagnosticsCannotFollowSymlink(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "security-dump")
	claim(t, s, d)
	target := t.TempDir()
	if err := os.Symlink(target, s.dumpDir); err != nil {
		t.Fatal(err)
	}
	changed := d
	caps, call := *d.Capabilities, *d.ProviderCall
	changed.Capabilities, changed.ProviderCall = &caps, &call
	changed.ProviderCall.Model = "conflicting"
	changed.Capabilities.Model = "conflicting"
	if created, err := s.ClaimAdmissionDispatch(accountingContext, changed, accountingOptions()); created || err == nil {
		t.Fatal("dump failure weakened conflict rejection")
	}
	files, err := os.ReadDir(target)
	if err != nil || len(files) != 0 {
		t.Fatal("diagnostics escaped private data root", err)
	}
	if resultCount(t, s, "provider_calls") != 1 || resultCount(t, s, "admission_dispatch_claims") != 1 {
		t.Fatal("diagnostic failure changed dispatch")
	}
}
