package overview

import (
	"bufio"
	"os"
	"path/filepath"
	"fmt"
	"github.com/go-while/go-utils"
	"log"
	"strings"
	"sync"
)

func CMD_NewOverviewIndex(file *string, group *string) bool {
	if file == nil {
		log.Printf("Error CMD_NewOverviewIndex file=nil")
		return false
	}
	if !utils.FileExists(*file) {
		log.Printf("Error CMD_NewOverviewIndex OV not found fp='%s'", *file)
		return false
	}
	OV_Index_File := fmt.Sprintf("%s.Index", *file)
	if utils.FileExists(OV_Index_File) {
		log.Printf("Error CMD_NewOverviewIndex OV_Index_File exists fp='%s'", OV_Index_File)
		return false
	}
	var a uint64 = 1
	var b uint64
	fields := "NewOVI"
	log.Printf("CMD_NewOverviewIndex: fp='%s'", *file)
	_, err := Scan_Overview(file, group, &a, &b, &fields, nil, "", nil)
	if err != nil {
		log.Printf("Error CMD_NewOverviewIndex Scan_Overview err='%v'", err)
		return false
	}
	return true
} // end func CMD_NewOverviewIndex

func WriteOverviewIndex(file *string, msgnums []uint64, offsets map[uint64]int64) {
	if offsets == nil {
		log.Printf("Error WriteOverviewIndex fp='%s' offsets=nil", filepath.Base(*file))
		return
	}
	//OV_IndexTable := fmt.Sprintf("%s.Dir")
	OV_Index_File := fmt.Sprintf("%s.Index", *file)
	//a, b, y, z := 0, 0, 0, 0
	var a, b uint64
	var y, z int64
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
	WriteOverviewIndex_INDEX(&OV_Index_File, &IndexLine{ a: &a, b: &b, y: &y, z: &z})
	//WriteOverviewIndex_TABLE(&OV_IndexTable, a, b, len(*offsets))
} // end func NewOverviewIndex

type IndexLine struct {
	a *uint64 // msgnum
	b *uint64 // msgnum
	y *int64 // offset
	z *int64 // offset
}

func WriteOverviewIndex_INDEX(file *string, data *IndexLine) {
	fh, err := os.OpenFile(*file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	defer fh.Close()
	if err != nil {
		log.Printf("Error WriteOverviewIndex_INDEX fp='%s' err0='%v'", filepath.Base(*file), err)
		return
	}
	line := fmt.Sprintf("|%d|%d|%d|%d|", *data.a, *data.b, *data.y, *data.z)
	//line := fmt.Sprintf("|0x%03x|0x%03x|0x%06x|0x%06x|", *data.a, *data.b, *data.y, *data.z)
	_, err = fh.WriteString(line+LF)
	if err != nil {
		log.Printf("Error WriteOverviewIndex_INDEX fp='%s' err2='%v'", filepath.Base(*file), err)
	}
	log.Printf("WriteOverviewIndex_INDEX a=%d b=%d y=%d z=%d line='%s'", *data.a, *data.b, *data.y, *data.z, line)
} // end func WriteOverviewIndex_INDEX

func (ovi *OverviewIndex) ReadOverviewIndex(file *string, group string, a uint64, b uint64) int64 {
	cached_offset := ovi.GetOVIndexCacheOffset(group, a)
	if cached_offset > 0 {
		return cached_offset
	}

	offset := cached_offset
	fh, err := os.OpenFile(*file, os.O_RDONLY, 0444)
	defer fh.Close()
	if err != nil {
		log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' err0='%v'", group, filepath.Base(*file), err)
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
	var prev_xa uint64
	//var prev_xb uint64
	var prev_xy int64
	//var prev_xz int64
	for fileScanner.Scan() {
		lc++
		line := fileScanner.Text()
		if len(line) < 16 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' line<32 lc=%d ll=%d", group, filepath.Base(*file), lc, len(line))
			break
		}
		//if line[0] == '|' && line[len(line)-1] == '|' {
		x := strings.Split(line, "|")
		if len(x) != 6 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' len(x) != 6 lc=%d", group, filepath.Base(*file), lc)
			break
		}

		x_a := utils.Str2uint64(x[1])
		if x_a == 0 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' DECODE ERROR1 lc=%d", group, filepath.Base(*file), lc)
			break
		}
		if a < x_a {
			continue
		}

		x_b := utils.Str2uint64(x[2])
		if x_b == 0 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' DECODE ERROR2 lc=%d", group, filepath.Base(*file), lc)
			break
		}
		/*
		if b > x_b {
			continue
		}
		*/
		x_y := utils.Str2int64(x[3])
		if x_y == 0 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' DECODE ERROR3 lc=%d", group, filepath.Base(*file), lc)
			break
		}
		x_z := utils.Str2int64(x[4])
		if x_z == 0 {
			log.Printf("Error ReadOverviewIndex groups='%s' fp='%s' DECODE ERROR4 lc=%d", group, filepath.Base(*file), lc)
			break
		}
		if DEBUG_OV { log.Printf("ROVI group='%s' a=%d x_a=%d x_b=%d x_y=%d x_z=%d", group, a, x_a, x_b, x_y, x_y) }
		if a >= x_a && a <= x_b {
			if prev_xa > 0 && prev_xy > 0 {
				OVIndex.SetOVIndexCacheOffset(group, prev_xa, prev_xy)
				//OVIndex.SetOVIndexCacheOffset(group, prev_xb, prev_xz)
			}
			OVIndex.SetOVIndexCacheOffset(group, x_a, x_y)
			//OVIndex.SetOVIndexCacheOffset(group, x_b, x_z)
			offset = x_y
			break
		}
		prev_xa = x_a
		//prev_xb = x_b
		prev_xy = x_y
		//prev_xz = x_z
		//a, b, y, z = utils.Str2int(x[1]), utils.Str2int(x[2]), utils.Str2int(x[3]), utils.Str2int(x[4])
		//}
	}
	return offset
} // end func ReadOverviewIndex

type OverviewIndex struct {
	mux sync.RWMutex
	IndexMap map[string]map[uint64]int64  // data[group][msgnum]offset
	IndexCache []string
	IndexCacheSize int  // number of groups we cache an index for
}

func (ovi *OverviewIndex) SetOVIndexCacheOffset(group string, fnum uint64, offset int64) {
	log.Printf("SetOVIndexCacheOffset group='%s' fnum=%d offset=%d", group, fnum, offset)
	ovi.mux.Lock()
	defer ovi.mux.Unlock()
	if ovi.IndexMap == nil {
		ovi.IndexCacheSize = 4096
		ovi.IndexMap = make(map[string]map[uint64]int64, ovi.IndexCacheSize)
	}
	if ovi.IndexMap[group] == nil {
		// memoryleak! map without limit caches infinite amount of index offsets for group
		ovi.IndexMap[group] = make(map[uint64]int64)
	}
	// check if map is full
	if len(ovi.IndexMap[group]) == ovi.IndexCacheSize {
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
} // end func SetOVIndexCacheOffset

func (ovi *OverviewIndex) GetOVIndexCacheOffset(group string, a uint64) int64 {
	if a < 101 {
		return 0
	}
	// offets are created for every 100 messages in overview
	// 1|100|offset1|offset2
	// 101|200|offset1|offset2
	// 201|300|offset1|offset2
	// floor 'a' to full 100+1
	// example: a=151 floors to 101
	//          a=1234 floors to 1201
	floored := ((a / 100) * 100) + 1
	log.Printf("Try GetOVIndexCacheOffset group='%s' a=%d floored=%d", group, a, floored)

	var offset int64
	ovi.mux.RLock()
	defer ovi.mux.RUnlock()
	if ovi.IndexMap == nil {
		return 0
	}
	if ovi.IndexMap[group] == nil {
		log.Printf("GetOVIndexCacheOffset group='%s' not cached", group)
		return 0
	}
	if ovi.IndexMap[group][floored] > 0 {
		offset = ovi.IndexMap[group][floored]
		log.Printf("OK GetOVIndexCacheOffset group='%s' a=%d f=%d @offset=%d", group, a, floored, offset)
		return offset
	}
	return 0
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
					delete(ovi.IndexMap[group] , fnum)
				}
		}
	}
}
