package overview

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func test_preload() {
	preload_zero("PRELOAD_ZERO_1K")
	preload_zero("PRELOAD_ZERO_4K")
	preload_zero("PRELOAD_ZERO_128K")
	preload_zero("PRELOAD_ZERO_1M")
}

// snapshot copies the live file (what a kill -9 leaves on disk), opens the copy like the server
// and checks that it holds exactly 'committed' lines.
func snapshot_check(t *testing.T, live string, snapdir string, committed uint64, step string) {
	t.Helper()
	data, err := os.ReadFile(live)
	if err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(snapdir, test_hash+".overview")
	os.Remove(snap)
	if err := os.WriteFile(snap, data, 0644); err != nil {
		t.Fatal(err)
	}
	ovfh, err := open_ov_once("snap", test_hash, snap)
	if err != nil {
		t.Fatalf("%s committed=%d: snapshot does not open without repair: %v\n%s", step, committed, err, CheckOverview(snap, true).String())
	}
	next := ovfh.Last
	handle_close_ov("snap", ovfh, false, true, false)
	want := committed + 1
	if committed == 0 && next == 0 {
		want = 0 // new file, nothing written yet
	}
	if next != want {
		t.Fatalf("%s committed=%d: next article number=%d want %d", step, committed, next, want)
	}
	if rep := CheckOverview(snap, true); rep.Status != REPAIR_STATUS_OK || uint64(rep.Lines) != committed {
		t.Fatalf("%s committed=%d: %s", step, committed, rep.String())
	}
}

// writes lines the way GO_pi_ov does, across several grows, and checks a crash snapshot after every step.
func TestWriteGrowCrashSnapshots(t *testing.T) {
	test_preload()
	dir := t.TempDir()
	snapdir := t.TempDir()
	path := filepath.Join(dir, test_hash+".overview")
	if err := Create_ov("test", path, test_hash, 3); err != nil {
		t.Fatal(err)
	}
	ovfh, err := handle_open_ov("test", test_hash, path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot_check(t, path, snapdir, 0, "new")

	subject := strings.Repeat("x", 700)
	n := 400 // ~300KB: grows the 12KB file by 128K at least twice
	startsize := ovfh.Mmap_size
	for i := 0; i < n; i++ {
		if ovfh.Last == 0 {
			ovfh.Last = 1
		}
		line := fmt.Sprintf("%d\t%s %d\tf@x\tMon, 1 Jan 1996 00:00:00 GMT\t<%d@x>\t\t1\t1\tnntp\n", ovfh.Last, subject, i, i)
		new_ovfh, err, _ := Write_ov("test", ovfh, line, false, false, false, false)
		if err != nil {
			t.Fatalf("Write_ov line %d: %v", i, err)
		}
		if new_ovfh != nil && new_ovfh.Mmap_handle != nil {
			ovfh = new_ovfh
		}
		// crash between Write_ov and Update_Footer: the line is not committed
		snapshot_check(t, path, snapdir, uint64(i), fmt.Sprintf("after Write_ov %d", i))

		ovfh.Last++
		if _, err := Update_Footer("test", ovfh, "test"); err != nil {
			t.Fatalf("Update_Footer line %d: %v", i, err)
		}
		snapshot_check(t, path, snapdir, uint64(i+1), fmt.Sprintf("after Update_Footer %d", i))
	}
	if ovfh.Mmap_size < startsize+2*128*1024 {
		t.Fatalf("file did not grow twice: start=%d now=%d", startsize, ovfh.Mmap_size)
	}
	if err := handle_close_ov("test", ovfh, true, true, false); err != nil {
		t.Fatal(err)
	}
	if rep := CheckOverview(path, true); rep.Status != REPAIR_STATUS_OK || rep.Lines != n {
		t.Fatalf("final: %s", rep.String())
	}
}

// files grown by the old Grow_ov have bodyend/fend values of the old size: they must still open
func TestFooterWrongBodyend(t *testing.T) {
	body := lines_body(1, 2, 3)
	path := build_test_file(t, body, 4096, false, 0, 0)
	data, _ := os.ReadFile(path)
	size := len(data) + OV_RESERVE_END
	foot := fmt.Sprintf("%s%d,last=%d,Findex=%d,bodyend=%d,fend=%d,zeropad=%s,%s", FOOTER_BEG, 1, 4, OV_RESERVE_BEG+len(body), 999, 1127, ZERO_PATTERN, FOOTER_END)
	data = append(data, []byte(zerofill(foot, OV_RESERVE_END))...)
	if len(data) != size {
		t.Fatalf("size")
	}
	os.WriteFile(path, data, 0644)
	ovfh, err := open_ov_once("test", test_hash, path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if ovfh.Last != 4 {
		t.Fatalf("Last=%d", ovfh.Last)
	}
	handle_close_ov("test", ovfh, false, true, false)
}
