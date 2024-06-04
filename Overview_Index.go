package overview

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/go-while/go-utils"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	//"time"
)

func CMD_NewOverviewIndex(file string, group string) bool {
	var mux sync.Mutex
	mux.Lock()
	defer mux.Unlock()
	// file = "/ov/abcd.overview"
	if file == "" || group == "" {
		log.Printf("Error CMD_NewOverviewIndex file=nil||group=nil")
		return false
	}
	if !utils.FileExists(file) {
		log.Printf("Error CMD_NewOverviewIndex OV not found fp='%s'", file)
		return false
	}
	OV_Index_File := fmt.Sprintf("%s.Index", file)
	if utils.FileExists(OV_Index_File) {
		log.Printf("Error CMD_NewOverviewIndex OV_Index_File exists fp='%s'", OV_Index_File)
		return false
	}
	var a uint64 = 1
	var b uint64
	fields := "NewOVI"
	_, err := Scan_Overview(file, group, a, b, fields, nil, "", nil)
	if err != nil {
		//time.Sleep(time.Second)
		log.Printf("Error CMD_NewOverviewIndex Scan_Overview err='%v'", err)
		return false
	}
	log.Printf("OK CMD_NewOverviewIndex: fp='%s'", file)
	return true
} // end func CMD_NewOverviewIndex

func WriteOverviewIndex(file string, msgnums []uint64, offsets map[uint64]int64) {
	if offsets == nil {
		log.Printf("Error WriteOverviewIndex fp='%s' offsets=nil", filepath.Base(file))
		return
	}
	OV_Index_File := fmt.Sprintf("%s.Index", file)
	//OV_IndexTable := OV_Index_File+".Dir"
	var a, b uint64 // msgnum
	var y, z int64  // offset
	for _, msgnum := range msgnums {
		offset := offsets[msgnum]
		if a == 0 {
			a = msgnum
			b = a
			y = offset
			z = y
		}
		if msgnum > b {
			b = msgnum
			z = offset
		}
	}
	WriteOverviewIndex_INDEX(OV_Index_File, &IndexLine{a: a, b: b, y: y, z: z})
	//WriteOverviewIndex_TABLE(&OV_IndexTable, a, b, len(*offsets))
} // end func NewOverviewIndex

type IndexLine struct {
	a uint64 // msgnum a
	b uint64 // msgnum b
	y int64  // y is offset for a
	z int64  // z is offset for b
}

func WriteOverviewIndex_INDEX(file string, data *IndexLine) {
	fh, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	defer fh.Close()
	if err != nil {
		log.Printf("Error WriteOverviewIndex_INDEX fp='%s' err0='%v'", filepath.Base(file), err)
		return
	}
	line := fmt.Sprintf("|%d|%d|%d|%d|", data.a, data.b, data.y, data.z)
	//line := fmt.Sprintf("|0x%03x|0x%03x|0x%06x|0x%06x|", *data.a, *data.b, *data.y, *data.z)
	_, err = fh.WriteString(line + LF)
	if err != nil {
		log.Printf("Error WriteOverviewIndex_INDEX fp='%s' err2='%v'", filepath.Base(file), err)
	}

	//log.Printf("WriteOverviewIndex_INDEX a=%d b=%d y=%d z=%d line='%s'", data.a, data.b, data.y, data.z, line)

} // end func WriteOverviewIndex_INDEX

func (ovi *OverviewIndex) ReadOverviewIndex(file string, group string, a uint64, b uint64) int64 {
	// file == "*.Index"
	cached_offset := ovi.GetOVIndexCacheOffset(group, a) // from memory
	if cached_offset > 0 {
		return cached_offset
	}

	var offset int64
	fh, err := os.OpenFile(file, os.O_RDONLY, 0444)
	defer fh.Close()
	if err != nil {
		log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' err0='%v'", group, filepath.Base(file), err)
		if AUTOINDEX {
			fOV := strings.Replace(file, ".Index", "", 1)
			log.Printf("sending to OV_AUTOINDEX_CHAN")
			OV_AUTOINDEX_CHAN <- &NEWOVI{fOV: fOV, group: group}
			log.Printf("sent to OV_AUTOINDEX_CHAN")
		}
		return offset
	}
	fileScanner := bufio.NewScanner(fh)
	// default NewScanner uses 64K buffer
	// we asume an index line in file not to be longer than 128 bytes incl LF
	maxScan := 128
	buf := make([]byte, maxScan)
	fileScanner.Buffer(buf, maxScan)
	fileScanner.Split(bufio.ScanLines)
	lc := 0
	log.Printf("ReadOverviewIndex groups='%s' SCAN a=%d", group, a)
	//var prev_xb uint64
	//var prev_xz int64
	/*  todo: get rid of x_a and x_y. only need x_b as floored + x_z offset
	 * 	|x_a|x_b|x_y|x_z
		|1|100|128|19736|
		|101|200|19932|41507|
		|201|300|41705|75295|
		|301|400|75566|99228|
		|401|500|99416|122651|
		|501|600|122871|146860|
		...
		|68801|68900|16551271|16578319|
		|68901|69000|16578768|16607605|
		|69001|69100|16608012|16637489|
		|69101|69200|16637818|16667846|
		|69201|69279|16668263|16690640|
		*
		* // new index format idea:
		* |100|19736|
		* |200|41507|
		* ...
		* |69200|16667846|
		* and ommit last one to save a line: |69279|16690640|
	*/
	for fileScanner.Scan() {
		lc++
		line := fileScanner.Text()
		if len(line) < 1 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' line<1 lc=%d ll=%d", group, filepath.Base(file), lc, len(line))
			break
		}
		if line[0] != '|' && line[len(line)-1] != '|' {
			break
		}
		x := strings.Split(line, "|")
		if len(x) != 6 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' len(x) != 6 lc=%d", group, filepath.Base(file), lc)
			break
		}

		x_a := utils.Str2uint64(x[1])
		if x_a == 0 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' DECODE ERROR1 lc=%d", group, filepath.Base(file), lc)
			break
		}
		if a < x_a {
			continue
		}

		x_b := utils.Str2uint64(x[2])
		if x_b == 0 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' DECODE ERROR2 lc=%d", group, filepath.Base(file), lc)
			break
		}
		/*
			if b > x_b {
				continue
			}
		*/
		x_y := utils.Str2int64(x[3])
		if x_y == 0 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' DECODE ERROR3 lc=%d", group, filepath.Base(file), lc)
			break
		}
		x_z := utils.Str2int64(x[4])
		if x_z == 0 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' DECODE ERROR4 lc=%d", group, filepath.Base(file), lc)
			break
		}
		if DEBUG_OV {
			log.Printf("ROVI group='%s' a=%d x_a=%d x_b=%d x_y=%d x_z=%d", group, a, x_a, x_b, x_y, x_y)
		}

		// memoryleak? always cache index offsets
		OVIndex.SetOVIndexCacheOffset(group, x_b, x_z)

		if a >= x_a && a <= x_b {
			// cache previous index offset
			//if prev_xb > 0 && prev_xz > 0 {
			//	OVIndex.SetOVIndexCacheOffset(group, prev_xb, prev_xz)
			//}
			// cache only index matches
			//OVIndex.SetOVIndexCacheOffset(group, x_b, x_z)
			offset = x_y
			break
		}
		//prev_xb = x_b
		//prev_xz = x_z
	}
	return offset
} // end func ReadOverviewIndex

type OverviewIndex struct {
	mux            sync.Mutex
	IndexMap       map[string]map[uint64]int64 // data[group][msgnum]offset
	IndexCache     []string                    // rotating list with cached index groups
	IndexCacheSize int                         // number of groups we cache an index for
}

func (ovi *OverviewIndex) SetOVIndexCacheOffset(group string, fnum uint64, offset int64) {
	log.Printf("SetOVIndexCacheOffset group='%s' fnum=%d offset=%d", group, fnum, offset)
	ovi.IndexCacheSize = 4096 // *hardcoded* keeps indexed offsets for this amount of groups in cache

	ovi.mux.Lock()
	if ovi.IndexMap == nil {
		ovi.IndexMap = make(map[string]map[uint64]int64, ovi.IndexCacheSize)
	}
	if ovi.IndexMap[group] == nil {
		// memoryleak! map without limit caches infinite amount of index offsets for group
		ovi.IndexMap[group] = make(map[uint64]int64)
	}

	// check if map is full
	if len(ovi.IndexMap) == ovi.IndexCacheSize {
		delgroup := ovi.IndexCache[0]
		ovi.IndexCache = ovi.IndexCache[1:] // pops [0]
		if delgroup != group {
			delete(ovi.IndexMap, delgroup)
		} else {
			ovi.IndexCache = append(ovi.IndexCache, delgroup) // re-append to top
		}
	}
	ovi.IndexMap[group][fnum] = offset
	ovi.IndexCache = append(ovi.IndexCache, group)
	ovi.mux.Unlock()
} // end func SetOVIndexCacheOffset

func (ovi *OverviewIndex) GetOVIndexCacheOffset(group string, a uint64) (offset int64) {
	if a < 100 {
		return
	}
	// offets are created for every 100 messages in overview
	// 1|100|offset1|offset2
	// 101|200|offset1|offset2
	// 201|300|offset1|offset2
	// floor 'a' to full 100
	// example: a=151 floors to 100
	//          a=1234 floors to 1200
	floored := ((a / 100) * 100)
	//log.Printf("Try GetOVIndexCacheOffset group='%s' a=%d floored=%d", group, a, floored)

	ovi.mux.Lock()
	defer ovi.mux.Unlock()

	if ovi.IndexMap == nil {
		return
	}
	if ovi.IndexMap[group] == nil {
		log.Printf("GetOVIndexCacheOffset group='%s' not cached", group)
		return
	}

	if ovi.IndexMap[group][floored] > 0 {
		offset = ovi.IndexMap[group][floored]
		log.Printf("OK GetOVIndexCacheOffset group='%s' a=%d f=%d @offset=%d", group, a, floored, offset)
	}
	return
} // func GetOVIndexCacheOffset

func (ovi *OverviewIndex) MemDropIndexCache(group string, fnum uint64) {
	log.Printf("MemDropIndexCache group='%s' fnum=%d", group, fnum)
	ovi.mux.Lock()
	defer ovi.mux.Unlock()
	if group == "" {
		// drop all cached index offsets
		ovi.IndexMap = make(map[string]map[uint64]int64, ovi.IndexCacheSize)
		ovi.IndexCache = []string{}
	} else {
		// drop index cache for group
		switch fnum {
		case 0:
			// fnum is not set
			// memoryleak! map without limit caches infinite amount of index offsets for group
			ovi.IndexMap[group] = make(map[uint64]int64)
		default:
			if fnum > 0 {
				delete(ovi.IndexMap[group], fnum)
			}
		}
	}
} // end func MemDropIndexCache

type NEWOVI struct {
	fOV   string
	group string
}

func OV_AutoIndex() {
	for {
		select {
		case dat := <-OV_AUTOINDEX_CHAN:
			if dat == nil || dat.fOV == "" || dat.group == "" {
				continue
			}
			log.Printf("OV_AutoIndexer: dat.fOV='%s' group='%s'", dat.fOV, dat.group)
			CMD_NewOverviewIndex(dat.fOV, dat.group)
		} // end select
	} // end for
} // end func OV_Indexer

func ReOrderOverview(file string, group string) bool {
	if strings.HasSuffix(group, ".test") {
		return false
	}
	debug := false
	newfile := file + ".new"
	if utils.FileExists(newfile) {
		if debug {
			log.Printf("Error ReOrderOverview FileExists newfile='%s'", newfile)
		}
		return false
	}
	if !utils.FileExists(file) {
		log.Printf("Error ReOrderOverview !FileExists file='%s'", file)
		return false
	}
	var a, b uint64
	a = 1
	fields := "ReOrderOV"
	lines, err := Scan_Overview(file, "", a, b, fields, nil, "", nil)
	ll := len(lines)
	if err != nil || ll == 0 {
		log.Printf("Error OV ReOrderOverview file='%s' err='%v' ll=%d", filepath.Base(file), err, ll)
	}
	mapdata := make(map[int64][]string)
	unixstamps := []int64{}
	uniq_msgids := make(map[string]bool)
	uniq_stamps := make(map[int64]bool)
	var header string
	var footer []string
	var readfooter bool
readlines:
	for i, line := range lines {
		if line == "" {
			log.Printf("Error OV ReOrderOverview i=%d line=nil ll=%d", i, ll)
			return false
		}

		if i == 0 {
			header = line
			continue
		}
		if debug {
			log.Printf("OV ReOrderOverview file='%s' *line='%v' ll=%d", filepath.Base(file), line, len(line))
		}
		if !readfooter && len(line) > 0 && string(line)[0] == 0 {
			readfooter = true
			continue
		}
		if readfooter {
			footer = append(footer, line)
			continue
		}

		datafields := strings.Split(line, "\t")
		if len(datafields) != OVERVIEW_FIELDS {
			log.Printf("Error OV ReOrderOverview file='%s' len(datafields)=%d != OVERVIEW_FIELDS=%d i=%d", filepath.Base(file), len(datafields), OVERVIEW_FIELDS, i)
			return false
		}

		//if !isvalidmsgid(datafields[4], true) {
		//	//if len(datafields[4]) > 0 && (datafields[4][0] == 'X' || datafields[4][0] == 0) { // check if first char is X or NUL
		//	//	// expiration removed article from overview
		//	//	continue
		//	//}
		//	log.Printf("Error OV ReOrderOverview file='%s' lc=%d field[4] err='!isvalidmsgid' f4='%s' f8='%s'", filepath.Base(file), i, datafields[4], datafields[8])
		//	return false
		//}

		//msgnum := utils.Str2uint64(datafields[0])

		msgid := datafields[4]
		switch uniq_msgids[msgid] {
		case true:
			if debug {
				log.Printf("Ignore Duplicate msgid='%s' file='%s' i=%d", msgid, filepath.Base(file), i)
			}
			//time.Sleep(time.Second)
			continue readlines
		case false:
			uniq_msgids[msgid] = true
		}

		unixepoch, err := ParseDate(datafields[3])
		if err != nil {
			log.Printf("IGNORE Error OV ReOrderOverview ParseDate file='%s' i=%d err='%v' unixepoch=%d msgid='%s' subj='%s' xref='%s'", filepath.Base(file), i, err, unixepoch, msgid, datafields[1], datafields[8])
			//return false
			continue readlines
		}
		mapdata[unixepoch] = append(mapdata[unixepoch], line)

		switch uniq_stamps[unixepoch] {
		case true:
			continue
		case false:
			uniq_stamps[unixepoch] = true
		}
		unixstamps = append(unixstamps, unixepoch)
	}

	sort.Sort(AsortFuncInt64(unixstamps))
	//l := len(unixstamps)
	var new_msgnum uint64 = 1
	var writeLines []string
	for _, timestamp := range unixstamps {
		//log.Printf("ReOrderOV timestamp=%d i=%d/l=%d", timestamp, i, l)
		if debug && len(mapdata[timestamp]) > 1 {
			log.Printf("ReOrderOV timestamp=%d lmap=%d", timestamp, len(mapdata[timestamp]))
		}
		for _, line := range mapdata[timestamp] {
			datafields := strings.Split(line, "\t")
			old_msgnum := utils.Str2uint64(datafields[0])
			if old_msgnum <= 0 {
				continue
			}
			subj := datafields[1]
			from := datafields[2]
			date := datafields[3]
			msgid := datafields[4]
			//xref := ""
			full_xref_str := datafields[8]
			var new_xrefs []string
			// check xrefs
			xrefs := strings.Split(full_xref_str, " ")
			// first xref has to be nntp, then group:n
			if len(xrefs) >= 2 && xrefs[0] == "nntp" {
				// loop over all xrefs we have
			loop_xrefs:
				for x := 1; x < len(xrefs); x++ {

					axref := xrefs[x]
					xrefdata := strings.Split(axref, ":")
					len_xrefdata := len(xrefdata)

					if len_xrefdata != 2 {
						log.Printf("Error ReOrderOV len_xrefdata != 2 old=%d new=%d msgid='%s'", old_msgnum, new_msgnum, msgid)
						continue loop_xrefs
					}

					xrefgroup := xrefdata[0]
					if group != xrefgroup {
						log.Printf("WARN ReOrderOV IGNORE xrefgroup='%s' msgid='%s'", xrefgroup, msgid)
						continue loop_xrefs
					}

					new_xrefs = append(new_xrefs, fmt.Sprintf("%s:%d", xrefgroup, new_msgnum))
				}
			}
			//parsedTime := time.Unix(timestamp, 0)
			//rfc5322date := parsedTime.Format(time.RFC1123Z)
			//date := rfc5322date

			if old_msgnum != new_msgnum {
				if debug {
					log.Printf("ReOrderOV old_msgnum=%d -> new_msgnum=%d date='%s' msgid='%s'", old_msgnum, new_msgnum, date, msgid)
				}
			}

			if spamfilter(subj, "subj", msgid) {
				log.Printf("ReOrderOV IGNORED msgid='%s' spamfilter 'subj'", msgid)
				continue
			}

			if spamfilter(from, "from", msgid) {
				log.Printf("ReOrderOV IGNORED msgid='%s' spamfilter 'from'", msgid)
				continue
			}

			doLimitBytes := false
			if doLimitBytes {
				bytes := utils.Str2uint64(datafields[6])
				var limit_bytes uint64 = 1024 * 1024
				if bytes > limit_bytes {
					log.Printf("ReOrderOV IGNORED msgid='%s' bytes=%d", msgid, bytes)
					continue
				}
			}

			new_xref := "nntp"
			for x := 0; x < len(new_xrefs); x++ {
				new_xref = new_xref + " " + new_xrefs[x]
			}
			newline := fmt.Sprintf("%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", new_msgnum, subj, from, date, msgid, datafields[5], datafields[6], datafields[7], new_xref)
			writeLines = append(writeLines, newline)
			if debug {
				log.Printf("newline='%s'", newline)
			}
			new_msgnum++
		} // end for mapdata
	} // end for timestamps

	if len(header) <= 0 || len(header) > 128 || len(footer) != 3 {
		log.Printf("Error OV ReOrderOverview head=%d foot=%d file='%s'", len(header), len(footer), filepath.Base(file))
		return false
	}
	if len(writeLines) > 0 {
		newfh, err := os.Create(newfile)
		if err != nil {
			log.Printf("Error OV ReOrderOverview os.Create(newfile='%s') err='%v'", filepath.Base(newfile), err)
			return false
		}
		fmt.Fprintf(newfh, "%s\n", header)
		for _, line := range writeLines {
			if line == "" {
				return false
			}
			fmt.Fprintf(newfh, "%s\n", line)
		}
		fmt.Fprintf(newfh, "\x00")
		err = newfh.Close()
		if err != nil {
			log.Printf("Error OV ReOrderOV newfh='%s' err='%v'", filepath.Base(newfile), err)
			return false
		}
		log.Printf("wrote %d lines to newfh='%s'", len(writeLines), filepath.Base(newfile))
		who := "ReOrderOV"
		debug_rescan := false
		var db *sql.DB = nil
		retbool, last := Rescan_Overview(who, newfile, group, 999, debug_rescan, db, nil)
		if retbool {
			log.Printf("OK ReOrderOV Rescan_Overview newfh='%s' retbool=%t last=%d", filepath.Base(newfile), retbool, last)
			return true
		}
		log.Printf("Error OV ReOrderOV Rescan_Overview newfh='%s' retbool=%t last=%d", filepath.Base(newfile), retbool, last)

	}
	return false
} // end func ReOrderOverview

type AsortFuncInt64 []int64

func (nf AsortFuncInt64) Len() int      { return len(nf) }
func (nf AsortFuncInt64) Swap(i, j int) { nf[i], nf[j] = nf[j], nf[i] }
func (nf AsortFuncInt64) Less(i, j int) bool {
	return nf[i] < nf[j]
}
