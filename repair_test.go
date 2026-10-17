package overview

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const test_hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func test_header() []byte {
	return []byte(zerofill(fmt.Sprintf("%s%d,group=%s,zeropad=%s,%s", HEADER_BEG, 1, test_hash, ZERO_PATTERN, HEADER_END), OV_RESERVE_BEG))
}

func test_line(num uint64) string {
	return fmt.Sprintf("%d\tSubject %d\tfrom@example.com\tMon, 1 Jan 1996 00:00:00 GMT\t<%d@example.com>\t <ref@example.com>\t1234\t10\tnntp\n", num, num, num)
}

// build_test_file writes header + body + free space + footer (if footer=true) and returns the path.
func build_test_file(t *testing.T, body string, free int, footer bool, last uint64, findex int) string {
	t.Helper()
	var b bytes.Buffer
	b.Write(test_header())
	b.WriteString(body)
	b.Write(make([]byte, free))
	if footer {
		size := b.Len() + OV_RESERVE_END
		if findex < 0 {
			findex = OV_RESERVE_BEG + len(body)
		}
		b.Write(build_footer(last, findex, size))
	}
	path := filepath.Join(t.TempDir(), test_hash+".overview")
	if err := os.WriteFile(path, b.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func lines_body(nums ...uint64) string {
	var s strings.Builder
	for _, n := range nums {
		s.WriteString(test_line(n))
	}
	return s.String()
}

// open_ok opens the file like the server does and returns Last (next number) and Findex.
func open_ok(t *testing.T, path string) (uint64, int) {
	t.Helper()
	ovfh, err := handle_open_ov("test", test_hash, path)
	if err != nil {
		t.Fatalf("handle_open_ov failed: %v", err)
	}
	last, findex := ovfh.Last, ovfh.Findex
	if err := handle_close_ov("test", ovfh, false, true, false); err != nil {
		t.Fatalf("handle_close_ov: %v", err)
	}
	return last, findex
}

func TestCheckOK(t *testing.T) {
	path := build_test_file(t, lines_body(1, 2, 3), 4096, true, 4, -1)
	if rep := CheckOverview(path, false); rep.Status != REPAIR_STATUS_OK {
		t.Fatalf("quick: %s", rep.String())
	}
	if rep := CheckOverview(path, true); rep.Status != REPAIR_STATUS_OK || rep.Lines != 3 || rep.LastNum != 3 {
		t.Fatalf("full: %s", rep.String())
	}
	if last, _ := open_ok(t, path); last != 4 {
		t.Fatalf("Last=%d want 4", last)
	}
}

func TestEmptyNewFile(t *testing.T) {
	path := build_test_file(t, "", 4096, true, 0, OV_RESERVE_BEG)
	if rep := CheckOverview(path, false); rep.Status != REPAIR_STATUS_OK {
		t.Fatalf("quick: %s", rep.String())
	}
	if rep := CheckOverview(path, true); rep.Status != REPAIR_STATUS_OK {
		t.Fatalf("full: %s", rep.String())
	}
}

// the old GO_pi_ov wrote the footer before Last++: last == last line number
func TestFooterOffByOne(t *testing.T) {
	path := build_test_file(t, lines_body(1, 2, 3), 4096, true, 3, -1)
	if rep := CheckOverview(path, false); rep.Status != REPAIR_STATUS_FIXABLE {
		t.Fatalf("quick: %s", rep.String())
	}
	if last, _ := open_ok(t, path); last != 4 {
		t.Fatalf("open: Last=%d want 4", last)
	}
	rep, err := RepairOverview(path, RepairOpts{Fix: true})
	if err != nil || rep.Status != REPAIR_STATUS_REPAIRED {
		t.Fatalf("repair: %v %s", err, rep.String())
	}
	if rep := CheckOverview(path, true); rep.Status != REPAIR_STATUS_OK {
		t.Fatalf("after repair: %s", rep.String())
	}
}

// crash inside the old Grow_ov: old footer NUL-filled, new footer not yet appended
func TestNoFooter(t *testing.T) {
	path := build_test_file(t, lines_body(1, 2, 3, 4), 8192, false, 0, 0)
	if rep := CheckOverview(path, false); rep.Status != REPAIR_STATUS_BROKEN {
		t.Fatalf("quick: %s", rep.String())
	}
	rep, err := RepairOverview(path, RepairOpts{Fix: true, Backup: true})
	if err != nil || rep.Status != REPAIR_STATUS_REPAIRED || rep.Lines != 4 || rep.LastNum != 4 {
		t.Fatalf("repair: %v %s", err, rep.String())
	}
	if rep.Backup == "" {
		t.Fatalf("no backup")
	}
	if _, err := os.Stat(rep.Backup); err != nil {
		t.Fatalf("backup missing: %v", err)
	}
	if last, findex := open_ok(t, path); last != 5 || findex != OV_RESERVE_BEG+len(lines_body(1, 2, 3, 4)) {
		t.Fatalf("open: Last=%d Findex=%d", last, findex)
	}
}

// handle_open_ov repairs a broken file itself
func TestAutoRepairOnOpen(t *testing.T) {
	path := build_test_file(t, lines_body(1, 2, 3), 8192, false, 0, 0)
	if last, _ := open_ok(t, path); last != 4 {
		t.Fatalf("Last=%d want 4", last)
	}
	baks, _ := filepath.Glob(path + ".bak.*")
	if len(baks) != 1 {
		t.Fatalf("expected one backup, got %v", baks)
	}
}

func TestAutoRepairDisabled(t *testing.T) {
	AutoRepair = false
	defer func() { AutoRepair = true }()
	path := build_test_file(t, lines_body(1, 2, 3), 8192, false, 0, 0)
	if _, err := handle_open_ov("test", test_hash, path); err == nil {
		t.Fatalf("open of a file without footer must fail with AutoRepair=false")
	}
}

// crash between Write_ov and Update_Footer: a line after Findex that the footer does not know
func TestStaleBytesAfterFindex(t *testing.T) {
	body := lines_body(1, 2)
	orphan := test_line(3)
	var b bytes.Buffer
	b.Write(test_header())
	b.WriteString(body)
	b.WriteString(orphan[:20]) // torn
	b.Write(make([]byte, 4096))
	size := b.Len() + OV_RESERVE_END
	b.Write(build_footer(3, OV_RESERVE_BEG+len(body), size))
	path := filepath.Join(t.TempDir(), test_hash+".overview")
	os.WriteFile(path, b.Bytes(), 0644)

	if rep := CheckOverview(path, false); rep.Status != REPAIR_STATUS_FIXABLE || rep.TrailingBytes != 20 {
		t.Fatalf("quick: %s", rep.String())
	}
	if last, findex := open_ok(t, path); last != 3 || findex != OV_RESERVE_BEG+len(body) {
		t.Fatalf("open: Last=%d Findex=%d", last, findex)
	}
	// open zeroed the stale bytes
	if rep := CheckOverview(path, false); rep.Status != REPAIR_STATUS_OK {
		t.Fatalf("after open: %s", rep.String())
	}
}

// crash inside the new grow_ov_safe after step 3: new footer at the end, old footer still in the body
func TestCrashInsideGrow(t *testing.T) {
	body := lines_body(1, 2, 3)
	var b bytes.Buffer
	b.Write(test_header())
	b.WriteString(body)
	b.Write(make([]byte, 1000))
	oldsize := b.Len() + OV_RESERVE_END
	b.Write(build_footer(4, OV_RESERVE_BEG+len(body), oldsize)) // old footer
	b.Write(make([]byte, 4096))
	newsize := b.Len() + OV_RESERVE_END
	b.Write(build_footer(4, OV_RESERVE_BEG+len(body), newsize)) // new footer
	path := filepath.Join(t.TempDir(), test_hash+".overview")
	os.WriteFile(path, b.Bytes(), 0644)

	if rep := CheckOverview(path, false); rep.Status != REPAIR_STATUS_FIXABLE {
		t.Fatalf("quick: %s", rep.String())
	}
	if last, _ := open_ok(t, path); last != 4 {
		t.Fatalf("open: Last=%d want 4", last)
	}
	if rep := CheckOverview(path, true); rep.Status != REPAIR_STATUS_OK {
		t.Fatalf("after open: %s", rep.String())
	}
}

func TestTornLineInMiddle(t *testing.T) {
	body := lines_body(1, 2) + "garbage without tabs\n" + lines_body(3, 4)
	path := build_test_file(t, body, 4096, true, 5, -1)
	rep, err := RepairOverview(path, RepairOpts{Fix: false})
	if err != nil || rep.Status != REPAIR_STATUS_BROKEN || rep.Invalid != 1 || rep.Lines != 4 {
		t.Fatalf("dry run: %v %s", err, rep.String())
	}
	rep, err = RepairOverview(path, RepairOpts{Fix: true})
	if err != nil || rep.Status != REPAIR_STATUS_REPAIRED {
		t.Fatalf("repair: %v %s", err, rep.String())
	}
	data, _ := os.ReadFile(path)
	if bytes.Contains(data, []byte("garbage")) {
		t.Fatalf("garbage line still in file")
	}
	if !bytes.Contains(data, []byte(test_line(4))) {
		t.Fatalf("line 4 lost")
	}
	if last, _ := open_ok(t, path); last != 5 {
		t.Fatalf("Last=%d want 5", last)
	}
}

func TestReusedNumbers(t *testing.T) {
	dup := strings.Replace(test_line(2), "Subject 2", "Other article", 1)
	body := lines_body(1, 2, 3) + dup + lines_body(4)
	path := build_test_file(t, body, 4096, true, 5, -1)
	rep, err := RepairOverview(path, RepairOpts{Fix: true})
	if err != nil || rep.Status != REPAIR_STATUS_REPAIRED || rep.Renumbered != 1 || rep.Lines != 5 || rep.LastNum != 5 {
		t.Fatalf("repair: %v %s", err, rep.String())
	}
	data, _ := os.ReadFile(path)
	if !bytes.Contains(data, []byte("5\tOther article")) {
		t.Fatalf("reused line not renumbered to 5")
	}
	if last, _ := open_ok(t, path); last != 6 {
		t.Fatalf("Last=%d want 6", last)
	}
}

func TestBrokenHeader(t *testing.T) {
	path := build_test_file(t, lines_body(1), 4096, true, 2, -1)
	data, _ := os.ReadFile(path)
	copy(data, []byte("XXXX"))
	os.WriteFile(path, data, 0644)
	rep, err := RepairOverview(path, RepairOpts{Fix: true})
	if err == nil || rep.Status != REPAIR_STATUS_BROKEN_HEADER {
		t.Fatalf("repair: %v %s", err, rep.String())
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(after, data) {
		t.Fatalf("file with broken header was changed")
	}
}

// file after clean_footer.sh: header + lines, nothing else
func TestCleanFooterOutput(t *testing.T) {
	path := build_test_file(t, lines_body(1, 2, 3), 0, false, 0, 0)
	rep, err := RepairOverview(path, RepairOpts{Fix: true})
	if err != nil || rep.Status != REPAIR_STATUS_REPAIRED || rep.Lines != 3 {
		t.Fatalf("repair: %v %s", err, rep.String())
	}
	if last, _ := open_ok(t, path); last != 4 {
		t.Fatalf("Last=%d want 4", last)
	}
}

func TestCleanOvField(t *testing.T) {
	in := "a\tb\rc\nd\x00e \xe4\xf6" // latin1 bytes must survive
	want := "a b c d e \xe4\xf6"
	if got := clean_ov_field(in); got != want {
		t.Fatalf("clean_ov_field=%q want %q", got, want)
	}
}

func TestExtractOverviewNoSpace(t *testing.T) {
	head := []string{
		"From:someone@example.com",
		"Subject:hello world",
		"Newsgroups: alt.test",
		"Date : Mon, 1 Jan 1996 00:00:00 GMT",
		"Message-ID:<x1@example.com>",
	}
	ovl := Extract_overview("<x1@example.com>", head)
	if ovl.Checksum != OVL_CHECKSUM {
		t.Fatalf("checksum=%d", ovl.Checksum)
	}
	if ovl.Subject != "hello world" || ovl.From != "someone@example.com" || ovl.Date != "Mon, 1 Jan 1996 00:00:00 GMT" {
		t.Fatalf("ovl=%+v", ovl)
	}
}
