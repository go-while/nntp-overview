package overview

/*
 * repair.go: check and repair broken overview files.
 *
 *  On-disk layout of an overview file:
 *    [0:128)            header  "#ov_init=<unix>,group=<hash>,zeropad=<NULs>,EOH\n"
 *    [128:Findex)       lines   "<num>\t<subj>\t<from>\t<date>\t<msgid>\t<refs>\t<bytes>\t<lines>\tnntp\n"
 *    [Findex:size-128)  NULs    free space
 *    [size-128:size)    footer  "\nEOV\ntime=<nano>,last=<next>,Findex=<off>,bodyend=..,fend=..,zeropad=<NULs>,\nEOF\n"
 *
 *  The footer's "last" is the NEXT article number (last line + 1).
 *
 *  RepairOverview rebuilds a file from its header and all valid lines:
 *    - torn lines and footer remains are dropped
 *    - lines with a number <= the highest number seen so far are moved
 *      to the end and get new numbers (keeps the file in ascending order)
 *    - a fresh zero block and footer are written
 *  The repaired file is written to <file>.repair.tmp and renamed over the
 *  original. With Backup, the original stays available as <file>.bak.<unixts>.
 *
 *  The server must not write to the file while it is repaired.
 *  The auto-repair in handle_open_ov holds the group lock while it runs.
 */

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/go-while/go-utils"
)

const (
	REPAIR_STATUS_OK            = "OK"            // nothing to do
	REPAIR_STATUS_FIXABLE       = "FIXABLE"       // footer 'last' off by one and/or stale bytes after Findex: fixed automatically on open
	REPAIR_STATUS_BROKEN        = "BROKEN"        // needs RepairOverview
	REPAIR_STATUS_BROKEN_HEADER = "BROKEN_HEADER" // header is invalid, not repaired automatically
	REPAIR_STATUS_REPAIRED      = "REPAIRED"      // RepairOverview rewrote the file
	REPAIR_STATUS_ERROR         = "ERROR"         // i/o error

	repair_pad_bytes = 128 * 1024 // free space after the last line of a repaired file
	repair_max_show  = 5          // number of problems listed per file
)

var (
	// AutoRepair enables RepairOverview when handle_open_ov can not open a file.
	AutoRepair bool = true
)

type RepairOpts struct {
	Fix    bool // write the repaired file (false: report only)
	Backup bool // keep the original as <file>.bak.<unixts>
}

type RepairReport struct {
	File          string
	Status        string
	Size          int64
	FooterOK      bool   // footer found and parsed
	FooterLast    uint64 // 'last' from footer (next number)
	FooterFindex  int    // 'Findex' from footer
	Lines         int    // valid lines
	Invalid       int    // dropped lines (torn, footer remains, bad format)
	Renumbered    int    // lines with a reused number, moved to the end
	TrailingBytes int    // non-NUL bytes between the first NUL and the footer
	FirstNum      uint64 // lowest article number
	LastNum       uint64 // highest article number (after renumbering)
	Problems      []string
	Backup        string // path of the backup file
}

func (r *RepairReport) problem(format string, a ...interface{}) {
	if len(r.Problems) < repair_max_show {
		r.Problems = append(r.Problems, fmt.Sprintf(format, a...))
	}
}

func (r RepairReport) String() string {
	str := fmt.Sprintf("%s %s size=%d lines=%d first=%d last=%d invalid=%d renumbered=%d trailing=%d footer=%t footer_last=%d footer_findex=%d",
		r.Status, r.File, r.Size, r.Lines, r.FirstNum, r.LastNum, r.Invalid, r.Renumbered, r.TrailingBytes, r.FooterOK, r.FooterLast, r.FooterFindex)
	if r.Backup != "" {
		str += " backup=" + r.Backup
	}
	for _, p := range r.Problems {
		str += "\n    " + p
	}
	return str
}

// parse_footer parses the 128 byte footer. returns ok=false if it is no valid footer.
func parse_footer(footer []byte) (last uint64, findex int, ok bool) {
	if len(footer) != OV_RESERVE_END {
		return 0, 0, false
	}
	if !bytes.HasPrefix(footer, []byte(FOOTER_BEG)) || !bytes.HasSuffix(footer, []byte(","+FOOTER_END)) {
		return 0, 0, false
	}
	foot := bytes.Split(footer, []byte(","))
	if len(foot) != SIZEOF_FOOT {
		return 0, 0, false
	}
	if !bytes.HasPrefix(foot[1], []byte("last=")) || !bytes.HasPrefix(foot[2], []byte("Findex=")) {
		return 0, 0, false
	}
	l, err1 := strconv.ParseUint(string(foot[1][len("last="):]), 10, 64)
	f, err2 := strconv.Atoi(string(foot[2][len("Findex="):]))
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return l, f, true
} // end func parse_footer

func check_header_bytes(header []byte) bool {
	return len(header) == OV_RESERVE_BEG &&
		bytes.HasPrefix(header, []byte(HEADER_BEG)) &&
		bytes.HasSuffix(header, []byte(HEADER_END))
} // end func check_header_bytes

// parse_ov_line checks one overview line without its trailing '\n'.
// returns the article number and true if the line is valid.
func parse_ov_line(line []byte) (uint64, bool) {
	if len(line) == 0 || bytes.IndexByte(line, 0) >= 0 {
		return 0, false
	}
	if bytes.Count(line, []byte("\t")) != OVERVIEW_TABS {
		return 0, false
	}
	numstr := line[:bytes.IndexByte(line, '\t')]
	num, err := strconv.ParseUint(string(numstr), 10, 64)
	if err != nil || num == 0 {
		return 0, false
	}
	return num, true
} // end func parse_ov_line

func build_footer(last uint64, findex int, size int) []byte {
	bodyend := size - OV_RESERVE_END
	foot_str := fmt.Sprintf("%s%d,last=%d,Findex=%d,bodyend=%d,fend=%d,zeropad=%s,%s", FOOTER_BEG, utils.Nano(), last, findex, bodyend, size, ZERO_PATTERN, FOOTER_END)
	return []byte(zerofill(foot_str, OV_RESERVE_END))
} // end func build_footer

// CheckOverview checks a file without changing it.
// quick: only header, footer, last line and the free space after Findex are checked.
// full: every line is checked, like RepairOverview without writing.
func CheckOverview(file_path string, full bool) RepairReport {
	if full {
		rep, _ := RepairOverview(file_path, RepairOpts{Fix: false})
		return rep
	}
	rep := RepairReport{File: file_path}
	fh, err := os.Open(file_path)
	if err != nil {
		rep.Status = REPAIR_STATUS_ERROR
		rep.problem("open: %v", err)
		return rep
	}
	defer fh.Close()
	st, err := fh.Stat()
	if err != nil {
		rep.Status = REPAIR_STATUS_ERROR
		rep.problem("stat: %v", err)
		return rep
	}
	size := st.Size()
	rep.Size = size
	if size < OV_RESERVE_BEG+OV_RESERVE_END {
		rep.Status = REPAIR_STATUS_BROKEN_HEADER
		rep.problem("file too small: %d bytes", size)
		return rep
	}

	header := make([]byte, OV_RESERVE_BEG)
	if _, err := fh.ReadAt(header, 0); err != nil || !check_header_bytes(header) {
		rep.Status = REPAIR_STATUS_BROKEN_HEADER
		rep.problem("invalid header")
		return rep
	}

	footer := make([]byte, OV_RESERVE_END)
	if _, err := fh.ReadAt(footer, size-OV_RESERVE_END); err != nil {
		rep.Status = REPAIR_STATUS_ERROR
		rep.problem("read footer: %v", err)
		return rep
	}
	last, findex, ok := parse_footer(footer)
	if !ok {
		rep.Status = REPAIR_STATUS_BROKEN
		rep.problem("no valid footer at end of file")
		return rep
	}
	rep.FooterOK, rep.FooterLast, rep.FooterFindex = true, last, findex
	bodyend := size - OV_RESERVE_END
	if int64(findex) < OV_RESERVE_BEG || int64(findex) >= bodyend {
		rep.Status = REPAIR_STATUS_BROKEN
		rep.problem("footer Findex=%d outside body [%d:%d)", findex, OV_RESERVE_BEG, bodyend)
		return rep
	}

	status := REPAIR_STATUS_OK

	// free space after Findex must be NUL
	free := make([]byte, bodyend-int64(findex))
	if _, err := fh.ReadAt(free, int64(findex)); err != nil {
		rep.Status = REPAIR_STATUS_ERROR
		rep.problem("read free space: %v", err)
		return rep
	}
	for _, c := range free {
		if c != 0 {
			rep.TrailingBytes++
		}
	}
	if rep.TrailingBytes > 0 {
		status = REPAIR_STATUS_FIXABLE
		rep.problem("%d stale bytes after Findex", rep.TrailingBytes)
	}

	if last == 0 && findex == OV_RESERVE_BEG {
		// new empty file
		rep.Status = status
		return rep
	}

	// read the last line before Findex
	window := int64(64 * 1024)
	start := int64(findex) - window
	if start < OV_RESERVE_BEG {
		start = OV_RESERVE_BEG
	}
	buf := make([]byte, int64(findex)-start)
	if _, err := fh.ReadAt(buf, start); err != nil {
		rep.Status = REPAIR_STATUS_ERROR
		rep.problem("read last line: %v", err)
		return rep
	}
	if len(buf) == 0 || buf[len(buf)-1] != '\n' {
		rep.Status = REPAIR_STATUS_BROKEN
		rep.problem("byte before Findex is not a newline")
		return rep
	}
	lastline := buf[:len(buf)-1]
	if i := bytes.LastIndexByte(lastline, '\n'); i >= 0 {
		lastline = lastline[i+1:]
	} else if start > OV_RESERVE_BEG {
		rep.Status = REPAIR_STATUS_BROKEN
		rep.problem("last line longer than %d bytes", window)
		return rep
	}
	num, valid := parse_ov_line(lastline)
	if !valid {
		rep.Status = REPAIR_STATUS_BROKEN
		rep.problem("last line is invalid")
		return rep
	}
	rep.LastNum = num
	switch {
	case num+1 == last:
		// ok
	case num == last:
		// written by the old code: footer was updated before Last++
		status = REPAIR_STATUS_FIXABLE
		rep.problem("footer last=%d equals last line number (old off-by-one)", last)
	default:
		rep.Status = REPAIR_STATUS_BROKEN
		rep.problem("footer last=%d does not match last line number=%d", last, num)
		return rep
	}
	rep.Status = status
	return rep
} // end func CheckOverview

// RepairOverview scans all lines of an overview file and rewrites it if needed.
// with opts.Fix=false it only reports what it would do.
func RepairOverview(file_path string, opts RepairOpts) (RepairReport, error) {
	rep := RepairReport{File: file_path}
	fail := func(status string, err error) (RepairReport, error) {
		rep.Status = status
		rep.problem("%v", err)
		return rep, err
	}

	fh, err := os.Open(file_path)
	if err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}
	defer fh.Close()
	st, err := fh.Stat()
	if err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}
	size := st.Size()
	rep.Size = size
	if size < OV_RESERVE_BEG {
		return fail(REPAIR_STATUS_BROKEN_HEADER, fmt.Errorf("file too small: %d bytes", size))
	}

	header := make([]byte, OV_RESERVE_BEG)
	if _, err := fh.ReadAt(header, 0); err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}
	if !check_header_bytes(header) {
		return fail(REPAIR_STATUS_BROKEN_HEADER, fmt.Errorf("invalid header"))
	}

	// the body ends at the footer if there is a valid one, else at EOF
	bodylimit := size
	if size >= OV_RESERVE_BEG+OV_RESERVE_END {
		footer := make([]byte, OV_RESERVE_END)
		if _, err := fh.ReadAt(footer, size-OV_RESERVE_END); err != nil {
			return fail(REPAIR_STATUS_ERROR, err)
		}
		if last, findex, ok := parse_footer(footer); ok {
			rep.FooterOK, rep.FooterLast, rep.FooterFindex = true, last, findex
			bodylimit = size - OV_RESERVE_END
		}
	}
	if !rep.FooterOK {
		rep.problem("no valid footer at end of file")
	}

	// output goes to a tmp file only when we fix
	var out *bufio.Writer
	var tmp *os.File
	tmp_path := file_path + ".repair.tmp"
	if opts.Fix {
		if tmp, err = os.OpenFile(tmp_path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, st.Mode().Perm()); err != nil {
			return fail(REPAIR_STATUS_ERROR, err)
		}
		defer func() {
			if tmp != nil {
				tmp.Close()
				os.Remove(tmp_path)
			}
		}()
		out = bufio.NewWriterSize(tmp, 1024*1024)
		if _, err := out.Write(header); err != nil {
			return fail(REPAIR_STATUS_ERROR, err)
		}
	}

	written := int64(OV_RESERVE_BEG)
	var maxnum uint64
	var conflicts [][]byte // lines with reused numbers, appended at the end
	firstnul := int64(-1)

	rd := bufio.NewReaderSize(io.NewSectionReader(fh, OV_RESERVE_BEG, bodylimit-OV_RESERVE_BEG), 1024*1024)
	pos := int64(OV_RESERVE_BEG)
	for firstnul < 0 {
		line, rerr := rd.ReadBytes('\n')
		if len(line) > 0 {
			linepos := pos
			pos += int64(len(line))
			if i := bytes.IndexByte(line, 0); i >= 0 {
				// free space starts here
				firstnul = linepos + int64(i)
				if i > 0 {
					rep.Invalid++
					rep.problem("torn line at offset %d: %q", linepos, short_str(string(line[:i]), 80))
				}
				break
			}
			if line[len(line)-1] != '\n' {
				// EOF without newline and without NUL
				rep.Invalid++
				rep.problem("unterminated line at offset %d: %q", linepos, short_str(string(line), 80))
				break
			}
			num, valid := parse_ov_line(line[:len(line)-1])
			if !valid {
				rep.Invalid++
				rep.problem("invalid line at offset %d: %q", linepos, short_str(string(line), 80))
			} else if num <= maxnum {
				rep.Renumbered++
				rep.problem("reused number %d at offset %d (highest so far %d)", num, linepos, maxnum)
				conflicts = append(conflicts, append([]byte(nil), line...))
			} else {
				if rep.FirstNum == 0 {
					rep.FirstNum = num
				}
				maxnum = num
				rep.Lines++
				if out != nil {
					if _, err := out.Write(line); err != nil {
						return fail(REPAIR_STATUS_ERROR, err)
					}
				}
				written += int64(len(line))
			}
		}
		if rerr == io.EOF {
			break
		} else if rerr != nil {
			return fail(REPAIR_STATUS_ERROR, rerr)
		}
	}

	// renumber conflicting lines to the end
	for _, line := range conflicts {
		maxnum++
		if rep.FirstNum == 0 {
			rep.FirstNum = maxnum
		}
		tab := bytes.IndexByte(line, '\t')
		newline := append([]byte(strconv.FormatUint(maxnum, 10)), line[tab:]...)
		rep.Lines++
		if out != nil {
			if _, err := out.Write(newline); err != nil {
				return fail(REPAIR_STATUS_ERROR, err)
			}
		}
		written += int64(len(newline))
	}
	rep.LastNum = maxnum

	// count non-NUL bytes between the first NUL and the footer
	if firstnul >= 0 {
		rest := io.NewSectionReader(fh, firstnul, bodylimit-firstnul)
		buf := make([]byte, 64*1024)
		for {
			n, rerr := rest.Read(buf)
			for _, c := range buf[:n] {
				if c != 0 {
					rep.TrailingBytes++
				}
			}
			if rerr == io.EOF {
				break
			} else if rerr != nil {
				return fail(REPAIR_STATUS_ERROR, rerr)
			}
		}
		if rep.TrailingBytes > 0 {
			rep.problem("%d non-NUL bytes after the first NUL (dropped)", rep.TrailingBytes)
		}
	}

	var newlast uint64
	if maxnum > 0 {
		newlast = maxnum + 1
	}
	newfindex := int(written)

	needs_repair := !rep.FooterOK || rep.Invalid > 0 || rep.Renumbered > 0 || rep.TrailingBytes > 0 ||
		rep.FooterLast != newlast || rep.FooterFindex != newfindex
	if rep.FooterOK && rep.FooterLast != newlast && rep.FooterLast != 0 {
		rep.problem("footer last=%d, lines say last=%d", rep.FooterLast, newlast)
	}
	if rep.FooterOK && rep.FooterFindex != newfindex {
		rep.problem("footer Findex=%d, lines end at %d", rep.FooterFindex, newfindex)
	}

	if !needs_repair {
		rep.Status = REPAIR_STATUS_OK
		return rep, nil
	}
	if !opts.Fix {
		rep.Status = REPAIR_STATUS_BROKEN
		return rep, nil
	}

	// free space + footer
	newsize := written + repair_pad_bytes + OV_RESERVE_END
	if _, err := out.Write(make([]byte, repair_pad_bytes)); err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}
	if _, err := out.Write(build_footer(newlast, newfindex, int(newsize))); err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}
	if err := out.Flush(); err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}
	if err := tmp.Close(); err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}

	// keep owner of the original file
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		_ = os.Chown(tmp_path, int(sys.Uid), int(sys.Gid))
	}

	if opts.Backup {
		// hardlink: the original stays reachable, file_path is never missing
		rep.Backup = fmt.Sprintf("%s.bak.%d", file_path, time.Now().Unix())
		if err := os.Link(file_path, rep.Backup); err != nil {
			rep.Backup = ""
			return fail(REPAIR_STATUS_ERROR, fmt.Errorf("backup link: %v", err))
		}
	}
	if err := os.Rename(tmp_path, file_path); err != nil {
		return fail(REPAIR_STATUS_ERROR, err)
	}
	tmp = nil // renamed, nothing to clean up
	sync_dir(filepath.Dir(file_path))

	// the index has offsets of the old file
	index_file := file_path + ".Index"
	if utils.FileExists(index_file) {
		_ = os.Rename(index_file, fmt.Sprintf("%s.bak.%d", index_file, time.Now().Unix()))
	}
	OVIndex.mux.Lock()
	has_cache := len(OVIndex.IndexMap) > 0
	OVIndex.mux.Unlock()
	if has_cache {
		OVIndex.MemDropIndexCache("", 0)
	}

	rep.Status = REPAIR_STATUS_REPAIRED
	rep.Size = newsize
	rep.FooterOK, rep.FooterLast, rep.FooterFindex = true, newlast, newfindex
	log.Printf("RepairOverview REPAIRED fp='%s' lines=%d last=%d invalid=%d renumbered=%d backup='%s'", file_path, rep.Lines, rep.LastNum, rep.Invalid, rep.Renumbered, rep.Backup)
	return rep, nil
} // end func RepairOverview

func sync_dir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
} // end func sync_dir

func short_str(s string, max int) string {
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
} // end func short_str
