package overview

import (
	"bufio"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/go-while/go-utils"
	"github.com/go-while/nntp-storage"
	"log"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

const (
	DEBUG_OV bool   = false
	null     string = "\x00"
	tab      string = "\010"
	CR       string = "\r"
	LF       string = "\n"
	CRLF     string = CR + LF

	XREF_PREFIX = "nntp"

	MAX_FLUSH int64 = 15 // flush mmaps every N seconds

	LIMIT_SPLITMAX_NEWSGROUPS int = 25

	SIZEOF_FOOT     int = 7
	OVL_CHECKSUM    int = 5
	OVERVIEW_FIELDS int = 9
	OVERVIEW_TABS   int = OVERVIEW_FIELDS - 1

	ZERO_PATTERN  string = "zerofill" // replace this pattern with zero padding
	ZERO_FILL_STR string = null       // zero pad with nul
	FILE_DELIM    string = "\n"       // string used as file delimiter

	OV_RESERVE_BEG = 128 // initially reserve n bytes in overview file
	OV_RESERVE_END = 128 // finally reserve n bytes in overview file

	//OV_LIMIT_MAX_LINE int = 1024               // limit stored overview line length //FIXME TODO
	//OV_FLUSH_EVERY_BYTES int = 128*1024     // flush memory mapped overview file every 128K (zfs recordsize?)     //FIXME TODO
	//OV_FLUSH_EVERY_SECS int64 = 60          // flush memory mapped overview file every 60s                        //FIXME TODO

	HEADER_BEG       string = string("#ov_init=")
	HEADER_END       string = string("EOH" + FILE_DELIM)
	BODY_END         string = string(FILE_DELIM + "EOV" + FILE_DELIM)
	FOOTER_BEG       string = string(BODY_END + "time=")
	FOOTER_END       string = string(FILE_DELIM + "EOF" + FILE_DELIM)
	ZERO_PATTERN_LEN int    = len(ZERO_PATTERN) // the len of the pattern
	ERR_OV_OVERFLOW  string = "ef01"            // write overview throws errstr 'ef01' when buffer is full and needs to grow

	CHECK_CHARS_LC string = "abcdefghijklmnopqrstuvwxyz0123456789"
)

var (
	PRELOAD_ZERO_1K     string
	PRELOAD_ZERO_4K     string
	PRELOAD_ZERO_128K   string
	PRELOAD_ZERO_1M     string
	open_mmap_overviews Open_MMAP_Overviews
	Known_msgids        Known_MessageIDs
	OV_handler          OV_Handler
	Overview            OV
	open_request_chan   chan Overview_Open_Request
	close_request_chan  chan Overview_Close_Request
	//read_request_chan         chan Overview_Read_Request  // FIXME TODO
	workers_done_chan       chan int      // holds an empty struct for done overview worker
	count_open_overviews    chan struct{} // holds an empty struct for every open overview file
	MAX_Open_overviews_chan chan struct{} // locking queue prevents opening of more overview files
)

type Overview_Open_Request struct {
	hash       string              // hash from group (hash.overview file)
	file_path  string              // grouphash.overview file path
	reply_chan chan Overview_Reply // replies back with the 'OVFH' or err
}

type Overview_Close_Request struct {
	force_close bool                // will be set to true if this overview has to be closed
	ovfh        *OVFH                // overview mmap file handle struct
	reply_chan  chan Overview_Reply // replies back with the 'OVFH' or err
}

type Overview_Reply struct {
	ovfh    *OVFH  // overview mmap file handle struct
	err     error // return error
	retbool bool  // returns a bool
}

type OVFH struct {
	File_path   string
	File_handle *os.File
	Mmap_handle mmap.MMap
	Mmap_size   int
	Mmap_range  int
	Time_open   int64
	Time_flush  int64
	Written     int
	Findex      int
	Last        uint64
	Hash        string
}

type OVL struct {
	// stores extracted overview line values
	/*
		"Subject:",
		"From:",
		"Date:",
		"Message-ID:",
		"References:",
		"Bytes:",
		"Lines:",
		"Xref:full",
	*/
	Subject        string
	From           string
	Date           string
	Messageid      string
	Messageidhash  string
	References     []string
	Bytes          int
	Lines          int
	Xref           string
	Newsgroups     []string
	Checksum       int // has to match OVL_CHECKSUM
	Retchan        chan []ReturnChannelData
	ReaderCachedir string
}

type ReturnChannelData struct {
	Retbool   bool
	Msgnum    uint64
	Newsgroup string
	Grouphash string
}

type OV struct {
	OVIC          chan OVL
	more_parallel bool
	signal_chans  map[int]chan struct{}
}

func (ov *OV) Load_Overview(maxworkers int, max_queue_size int, max_open_mmaps int, known_messageids int, ov_opener int, ov_closer int, close_always bool, more_parallel bool, stop_server_chan chan bool, debug_OV_handler bool) bool {
	/* Load_Overview has to be called with
	 *
	 * 1) maxworkers int            # number of workers to process incoming headers (recommended: = NumCPUs*2)
	 *
	 * 2) max_queue_size int        # limit the incoming queue (recommended: = maxworkers) >[never seen it getting full]
	 *
	 * 3) max_open_mmaps int        # limit max open memory mappings (recommended: = 2-768 tested)
	 *
	 * 4) max_known_msgids int      # cache of known messageidhashs. should be at least = cap(storage.WriteCache.wc_head_chan)
	 *
	 * 5) ov_opener int             # number of handlers for open_requests (recommended: = maxworkers)
	 *
	 * 6) ov_closer int             # number of handlers for close_requests (recommended: = maxworkers+1)
	 *
	 * 7) close_always              # bool: set to true if you want to close overview after every line
	 *
	 * 8) a stop_server_chan := make(chan bool, 1)
	 *   so we can send a true to the channel from our app and the worker(s) will close
	 *
	 * X) the return value of Load_Overview() -> Launch_overview_workers() is the input_channel: 'OVIC'
	 *      that's used to feed extracted ovl (OVL overview lines) to workers
	 */

	if stop_server_chan == nil {
		log.Printf("ERROR Load_Overview stop_server_chan=nil")
		os.Exit(1)
	}
	if maxworkers <= 0 {
		log.Printf("ERROR Load_Overview maxworkers<=0")
		os.Exit(1)
	}
	if ov_opener <= 0 {
		log.Printf("ERROR Load_Overview ov_opener<=0")
		os.Exit(1)
	}
	if ov_closer <= 0 {
		log.Printf("ERROR Load_Overview ov_closer<=0")
		os.Exit(1)
	}
	if max_open_mmaps < 2 {
		max_open_mmaps = 2
	}

	Overview.more_parallel = more_parallel

	log.Printf("Start Load_Overview: maxworkers=%d max_queue_size=%d max_open_mmaps=%d known_messageids=%d ov_opener=%d ov_closer=%d, more_parallel=%t, stop_server_chan='%v', debug_OV_handler=%t", maxworkers, max_queue_size, max_open_mmaps, known_messageids, ov_opener, ov_closer, more_parallel, stop_server_chan, debug_OV_handler)

	if MAX_Open_overviews_chan == nil {
		preload_zero("PRELOAD_ZERO_1K")
		preload_zero("PRELOAD_ZERO_4K")
		preload_zero("PRELOAD_ZERO_128K")
		// 'open_mmap_overviews' locks overview files per newsgroup in GO_pi_ov()
		// so concurrently running overview workers will not write at the same time to same overview/newsgroup
		// if a worker 'A' tries to write to a mmap thats already open by any other worker at this moment
		// worker A creates a channel in 'ch' and waits for the other worker to respond back over that channel
		// v: holds a true with "group" as key if overview is already open
		// ch: stores the channels from worker A with "group" as key, so the other worker holding the map
		//      will signal to that channel its unlocked
		open_mmap_overviews = Open_MMAP_Overviews{
			v:  make(map[string]int64, max_open_mmaps),
			ch: make(map[string][]chan struct{}, max_open_mmaps),
		}

		if known_messageids > 0 { // setup known_messageids map only if we want to
			Known_msgids = Known_MessageIDs{
				v:          make(map[string]bool, known_messageids),
				Debug:      debug_OV_handler,
				MAP_MSGIDS: known_messageids,
			}
		}
		// prefill channel with locks so we dont open more mmap files than this available objects in channel
		MAX_Open_overviews_chan = make(chan struct{}, max_open_mmaps)
		for i := 1; i <= max_open_mmaps; i++ {
			MAX_Open_overviews_chan <- struct{}{}
		}

		// create OV_Handler open/close_request_channels
		open_request_chan = make(chan Overview_Open_Request, max_open_mmaps)
		close_request_chan = make(chan Overview_Close_Request, max_open_mmaps)
		count_open_overviews = make(chan struct{}, max_open_mmaps)
		Overview.signal_chans = make(map[int]chan struct{}, max_open_mmaps)
		if DEBUG_OV {
			debug_OV_handler = true
		}
		OV_handler = OV_Handler{
			V:              make(map[string]OV_Handler_data, max_open_mmaps),
			Debug:          debug_OV_handler,
			STOP:           stop_server_chan,
			MAX_OPEN_MMAPS: max_open_mmaps,
			CLOSE_ALWAYS:   close_always,
		}

		log.Printf("Load_Overview maxworkers=%d, max_queue_size=%d, max_open_mmaps=%d, known_messageids=%d, ov_opener=%d, ov_closer=%d, stop_server_chan='%v' debug_OV_handler=%t len(MAX_Open_overviews_chan)=%d",
			maxworkers, max_queue_size, max_open_mmaps, known_messageids, ov_opener, ov_closer, stop_server_chan, debug_OV_handler, len(MAX_Open_overviews_chan))

		workers_done_chan = make(chan int, maxworkers)
		Overview.OVIC = make(chan OVL, max_queue_size) // one input_channel to serve them all with cap of max_queue_size

		// launch the overview worker routines
		for ov_wid := 1; ov_wid <= maxworkers; ov_wid++ {
			go ov.overview_Worker(ov_wid)
			utils.BootSleep()
		}

		// launch overview opener routines
		for ov_hid := 1; ov_hid <= ov_opener; ov_hid++ {
			Overview.signal_chans[ov_hid] = make(chan struct{}, 1)
			go OV_handler.Overview_handler_OPENER(ov_hid, open_request_chan)
			utils.BootSleep()
		}

		// launch overview closer routines
		for ov_hid := 1; ov_hid <= ov_closer; ov_hid++ {
			go OV_handler.Overview_handler_CLOSER(ov_hid, close_request_chan)
			utils.BootSleep()
		}

		// launch mmaps idle check
		go OV_handler.Check_idle()

		return true

	} else {
		log.Printf("ERROR: Load_Overview already created")
	}

	return false
} // end func Load_Overview

func Watch_overview_Workers(maxworkers int) {
	// run Watch_overview_Workers() whenever you're done feeding, before closing your app
	logstr := ""
	closed_Overview_OVIC := false
forever:
	for {
		time.Sleep(1 * time.Second)

		workers_done := len(workers_done_chan)               // int
		open_overviews := len(count_open_overviews)          // int
		queued_overviews := len(Overview.OVIC)               // int
		rc_head_chan := len(storage.WriteCache.WC_head_chan) // int
		wc_head_chan := len(storage.ReadCache.RC_head_chan)  // int
		wc_body_chan := len(storage.WriteCache.WC_body_chan) // int
		rc_body_chan := len(storage.ReadCache.RC_body_chan)  // int
		cache_history := len(storage.WriteCache.Log_cache_history_chan)		 // int
		xrefs := len(storage.XrefLinker.Xref_link_chan)      // int
		all_ov_workers_done := workers_done == maxworkers    // bool
		logstr = fmt.Sprintf("workers_done=%d/%d open_overviews=%d queued_overviews=%d wc_head_chan=%d wc_body_chan=%d rc_head_chan=%d rc_body_chan=%d cache_history=%d xrefs=%d", workers_done, maxworkers, open_overviews, queued_overviews, wc_head_chan, wc_body_chan, rc_head_chan, rc_body_chan, cache_history ,xrefs)



		// check if everything is empty
		if open_overviews == 0 && queued_overviews == 0 &&
			wc_head_chan == 0 && wc_body_chan == 0 &&
			rc_head_chan == 0 && rc_body_chan == 0 &&
			cache_history == 0 && xrefs == 0 {
				if !closed_Overview_OVIC {
					close(Overview.OVIC)
					closed_Overview_OVIC = true
				}
				if all_ov_workers_done {
					break forever
				}
		}
		log.Printf("WAIT Watch_overview_Workers %s", logstr)
	}
	log.Printf("DONE Watch_overview_Workers %s", logstr)
	time.Sleep(5 * time.Second)
} // end func watch_overview_Workers

func notify_workers_done_chan(ov_wid int) {
	log.Printf("overview_Worker %d) worker done", ov_wid)
	workers_done_chan <- ov_wid
}

func (ov *OV) overview_Worker(ov_wid int) {
	did, max := 0, 10000
	log.Printf("overview_Worker %d) Alive", ov_wid)

	stop := false
forever:
	for {
		if did >= max {
			break forever
		}
		select {
		case ovl, ok := <-Overview.OVIC:
			if !ok {
				log.Printf("overview_Worker %d) OVIC is closed", ov_wid)
				notify_workers_done_chan(ov_wid)
				stop = true
				break forever
			}
			if DEBUG_OV {
				log.Printf("overview_Worker %d) got ovl msgid='%s'", ov_wid, ovl.Messageid)
			}
			// handle incoming overview line
			// loop over the newsgroups and pass the ovl to every group
			ovl.Retchan <- ov.di_ov(ovl) // passes XXXXX to the app via the supplied Retchan
			//close(ovl.Retchan) // dont close and FIXME: try to reuse from frontend
			did++
		} // end select
	} // end for forever
	if !stop {
		go ov.overview_Worker(ov_wid)
	}
} // end func Overview_Worker

func fail_retchan(retchan chan ReturnChannelData) ReturnChannelData {
	if retchan != nil {
		retchan <- ReturnChannelData{}
	}
	return ReturnChannelData{}
} // end func fail_retchan

func true_retchan(msgnum uint64, newsgroup string, grouphash string, retchan chan ReturnChannelData) ReturnChannelData {
	if retchan != nil {
		retchan <- ReturnChannelData{true, msgnum, newsgroup, grouphash}
		return ReturnChannelData{} // dont return anything as it wont be read by anyone, data goes back via retchan
	}
	return ReturnChannelData{true, msgnum, newsgroup, grouphash}
} // end func true_retchan

func (ov *OV) di_ov(ovl OVL) []ReturnChannelData {
	// divide_incoming_overview
	dones := 0
	var retlist []ReturnChannelData
	var retchans []chan ReturnChannelData
	overviewline := Construct_OVL(ovl)

	for _, newsgroup := range ovl.Newsgroups {

		hash := strings.ToLower(utils.Hash256(newsgroup))

		if ov.more_parallel { // constantly spawning of new go routines eats memory

			retchan := make(chan ReturnChannelData, 1)
			retchans = append(retchans, retchan)
			go ov.GO_pi_ov(overviewline, newsgroup, hash, ovl.ReaderCachedir, retchan)

		} else {

			if retdata := ov.GO_pi_ov(overviewline, newsgroup, hash, ovl.ReaderCachedir, nil); retdata.Retbool == true {
				retlist = append(retlist, retdata)
				dones++
			}
		}

	} // end for range ovl.Newsgroups

	if ov.more_parallel {
		i := 0
		for {
			select {
			case retdata := <-retchans[i]:
				if retdata.Retbool {
					retlist = append(retlist, retdata)
					dones++
				}

			} // end select
			i++
			if i >= len(retchans) {
				break
			}
		} // end for
	} // end if ov.more_parallel

	/*
		dropchan := -1
		checked := 0
		forever:
		for { // FIXME strange double for loop to check channels

			test_chans:
			for i := 0; i < len(retchans); i++ {
				checked++
				select {
					// will block for response on first channel, break, remove and loop again,
					// maybe we got all done
					case retdata, ok := <- retchans[i]:
						if retdata.Retbool || !ok {
							dones++
							dropchan = i
							if retdata.Retbool {
								// sends retdata containing msgnum and newsgroup back to update activitymap
								retlist = append(retlist, retdata)
							}
							break test_chans
						}
				} // end select
			} // end for retchans

			// remove chan from test
			if dones == len(ovl.Newsgroups) {
				break forever
			}
			if dropchan != -1 && len(retchans) > 0 {
				retchans = append(retchans[:dropchan], retchans[dropchan+1:]...)
				dropchan = -1
			}
		} // end for forever
	*/

	if dones == len(ovl.Newsgroups) {
		if DEBUG_OV {
			log.Printf("di_ov dones=%d len_ovl.Newsgroups=%d retchans=%d retlist=%d", dones, len(ovl.Newsgroups), len(retchans), len(retlist))
		}
	} else {
		log.Printf("ERROR di_ov dones=%d len_ovl.Newsgroups=%d retchans=%d retlist=%d", dones, len(ovl.Newsgroups), len(retchans), len(retlist))
	}
	return retlist
} // end func divide_incoming_overview

func Construct_OVL(ovl OVL) string {
	// construct an overview line
	max_ref := 15
	ref_len, ref_len_limit := 0, 1024
	var references string
	for i, ref := range ovl.References {
		len_ref := len(ref)
		new_ref_len := ref_len + len_ref
		if new_ref_len > ref_len_limit {
			log.Printf("BREAK Construct_OVL new_ref_len=%d msgid='%s' i=%d", new_ref_len, ovl.Messageid, i)
			break
		}
		if i >= max_ref {
			log.Printf("BREAK Construct_OVL new_ref_len=%d msgid='%s' i=%d/%d", new_ref_len, ovl.Messageid, i ,len(ovl.References))
			break
		}
		references = references + " " + ref
		ref_len += len_ref

	}
	ret_ovl_str := fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%d\t%d", ovl.Subject, ovl.From, ovl.Date, ovl.Messageid, references, ovl.Bytes, ovl.Lines)
	return ret_ovl_str
} // end func Construct_OVL

func return_overview_lock() {
	MAX_Open_overviews_chan <- struct{}{}
} // end func return_overview_lock

func (ov *OV) GO_pi_ov(overviewline string, newsgroup string, hash string, cachedir string, retchan chan ReturnChannelData) ReturnChannelData {
	// GO_process_incoming_overview
	// GO_pi_ov can run concurrently!

	<-MAX_Open_overviews_chan    // get a global lock to limit total num of open overview files
	defer return_overview_lock() // return the lock whenever func returns

	var err error
	mmap_file_path := cachedir + "/" + hash + ".overview"

	ovfh, err := Open_ov(mmap_file_path)
	if err != nil || ovfh == nil {
		log.Printf("ERROR overview.Open_ov ovfh='%v 'err='%v'", ovfh, err)
		return fail_retchan(retchan)

	}
	if DEBUG_OV {
		log.Printf("overview.Open_ov ng='%s' OK", newsgroup)
	}

	if ovfh.Last == 0 {
		ovfh.Last = 1
	}

	xref := fmt.Sprintf(XREF_PREFIX+" %s:%d", newsgroup, ovfh.Last)
	ovl_line := fmt.Sprintf("%d\t%s\t%s\n", ovfh.Last, overviewline, xref)
	err, errstr := Write_ov(ovfh, ovl_line, false, false, false)
	if err != nil {
		log.Printf("ERROR Write_ovfh err='%v' errstr='%s'", err, errstr)
		return fail_retchan(retchan)

	} else {
		if DEBUG_OV {
			log.Printf("overview.Write_ov OK ng='%s' ovfh.Findex=%d ovfh.Last=%d", newsgroup, ovfh.Findex, ovfh.Last)
		}

		if err := Update_Footer(ovfh); err != nil {
			log.Printf("ERROR overview.Update_Footer ng='%s' err='%v'", newsgroup, err)
			return fail_retchan(retchan)

		} else {
			if DEBUG_OV {
				log.Printf("overview.Update_Footer OK ng='%s' ovfh.Findex=%d ovfh.Last=%d", newsgroup, ovfh.Findex, ovfh.Last)
			}
		}
	}
	ovfh.Last++

	// finally close the mmap
	if DEBUG_OV {
		log.Printf("p_i_o: Closing fp='%s'", ovfh.File_path)
	}
	if err := Close_ov(ovfh, true, false); err != nil {
		log.Printf("p_i_o: ERROR FINAL Close_ovfh err='%v'", err)
		return fail_retchan(retchan)

	}
	last_msgnum := ovfh.Last - 1
	return true_retchan(last_msgnum, newsgroup, hash, retchan)
} // end func GO_pi_ov

func Extract_overview(msgid string, header []string) OVL {
	var ovl OVL

	has_from, has_news, has_date, has_subj := false, false, false, false
	has_bytes, has_lines, has_msgid, has_refs, has_xref := false, false, false, false, false

	//header_key, header_key_L, header_dat := "", "", ""

	for i, headline := range header {

		spaced_line := false
		if len(headline) > 0 {
			spaced_line = utils.IsSpace(headline[0])
		}
		if spaced_line {
			continue
		}

		nextline := i + 1
		spaced_nextline := false
		str_nextline := ""
		//log.Printf("msgid='%s' line=%d", msgid, i)
		// get the next line if any
		if nextline < len(header)-1 {
			if header[nextline] != "" {
				str_nextline = header[nextline]                      // next line as string
				spaced_nextline = utils.IsSpace(header[nextline][0]) // first char of next line
			}
		}
		/*
			"Subject:",
			"From:",
			"Date:",
			"Message-ID:",
			"References:",
			"Bytes:",
			"Lines:",
			"Xref:full",
		*/

		header_key := strings.TrimSpace(strings.Split(headline, ":")[0])
		header_key_L := strings.ToLower(header_key)
		header_dat := strings.TrimSpace(strings.Replace(headline, header_key+": ", "", 1))

		if !has_subj && header_key_L == "subject" {
			has_subj = true
			ovl.Checksum++
			ovl.Subject = header_dat
			if spaced_nextline {
				ovl.Subject = ovl.Subject + " " + strings.TrimSpace(str_nextline)
			}
		} else if !has_from && header_key_L == "from" {
			has_from = true
			ovl.Checksum++
			ovl.From = header_dat
			if spaced_nextline {
				ovl.From = ovl.From + " " + strings.TrimSpace(str_nextline)
			}
		} else if !has_date && header_key_L == "date" {
			has_date = true
			ovl.Checksum++
			ovl.Date = header_dat
			/*if spaced_nextline {
				ovl.Date += strings.TrimSpace(str_nextline)
			}*/
		} else if !has_msgid && header_key_L == "message-id" {
			if !has_msgid && msgid == "?" && isvalidmsgid(header_dat, false) {
				msgid = header_dat
				ovl.Messageid = msgid
				has_msgid = true
				ovl.Checksum++
			} else if !has_msgid && msgid != "?" && msgid == header_dat {
				ovl.Checksum++
				has_msgid = true
				ovl.Messageid = header_dat
				/*
					if spaced_nextline {
						ovl.Messageid += strings.TrimSpace(str_nextline)
					}
				*/

			} else {
				log.Printf("ERROR Extract_overview msgid='%s' != header_dat='%s'", msgid, header_dat)
				continue
			}
			ovl.Messageid = strings.TrimSpace(ovl.Messageid)
		} else if !has_refs && header_key_L == "references" {
			has_refs = true
			//ovl.Checksum++
			//ovl.References = header_dat
			references := header_dat
			//if spaced_nextline {
			//    references += str_nextline
			//}
			//references = strings.Replace(ovl.Xref, "  ", " ", -1)
			//references = strings.Replace(references, "  ", " ", -1)
			//references = strings.Replace(references, "  ", " ", -1)
			ovl.References = Split_References(references)

		} else
		/*
			if !has_bytes && strings.HasPrefix(headline, "Bytes: ") {
				has_bytes = true
				ovl.Bytes = utils.Str2int(header_dat)
				ovl.Checksum++

			} else
			if !has_lines && strings.HasPrefix(headline, "Lines: ") {
				has_lines = true
				ovl.Lines = utils.Str2int(header_dat)
				if ovl.Lines > 0 {
					ovl.Checksum++
				}

			} else
		*/
		if !has_xref && header_key_L == "xref" {
			has_xref = true
			ovl.Xref = header_dat
			if spaced_nextline {
				ovl.Xref += str_nextline
				ovl.Xref = strings.Replace(ovl.Xref, "  ", " ", -1)
			}
			//ovl.Checksum++

		} else if !has_news && header_key_L == "newsgroups" {
			newsgroups_str := header_dat
			if spaced_nextline {
				newsgroups_str += strings.TrimSpace(str_nextline)
			}
			if DEBUG_OV {
				log.Printf("extract_overview msgid='%s' newsgroups='%s'", msgid, newsgroups_str)
			}
			ovl.Newsgroups = Split_NewsGroups(msgid, newsgroups_str)
			if len(ovl.Newsgroups) > 0 {
				has_news = true
				ovl.Checksum++
			}
		}
	} // end for

	if DEBUG_OV || ovl.Checksum != OVL_CHECKSUM {
		log.Printf("Extract_overview ovl.Checksum=%d/%d msgid='%s' has_from=%t has_news=%t has_date=%t has_subj=%t has_bytes=%t has_lines=%t has_msgid=%t has_refs=%t has_xref=%t",
			ovl.Checksum, OVL_CHECKSUM, msgid, has_from, has_news, has_date, has_subj, has_bytes, has_lines, has_msgid, has_refs, has_xref)

		if !has_msgid {
			Print_lines(header)
		}
	}
	return ovl
} // end func Extract_overview

func Print_lines(lines []string) {
	for i, line := range lines {
		log.Printf("line=%d) %s", i, line)
	}
}

func Split_References(astring string) []string {
	if DEBUG_OV {
		log.Printf("Split_References input='%s'", astring)
	}
	var references []string
	limit, did := 25, 0
	if len(astring) > 0 {

		astring = strings.Replace(astring, ",", " ", -1)
		astring = strings.Replace(astring, "  ", " ", -1)
		astring = strings.Replace(astring, "  ", " ", -1)
		references_dirty := strings.Split(astring, " ")
		e, j := 0, 0
		for i, ref := range references_dirty {
			did++
			if did >= limit {
				break
			}
			aref := strings.TrimSpace(ref)
			if isvalidmsgid(aref, true) {
				references = append(references, aref)
				j++
			} else {
				if DEBUG_OV {
					log.Printf("ERROR Split_References !isvalidmsgid='%s' i=%d", aref, i)
				}
				e++
			}
		}
		if DEBUG_OV {
			log.Printf("Split_References input='%s' returned=%d e=%d j=%d", astring, len(references), e, j)
		}

	} else {

	}
	return references
} // end func Split_References

func Clean_Headline(msgid string, line string, debug bool) string {
	/*
		retline := ""

		cleaned := 0

		for _, c := range line {
			if c >= 32 {
				retline = retline+string(c)
			} else {
				cleaned++
			}
		}


		if cleaned > 0 && msgid != "?" {
			log.Printf("Clean_Headline msgid='%s' cleaned=%d", msgid, cleaned)
		}
	*/

	if strings.Contains(line, "\x00") {
		if debug {
			log.Printf("Removed Nul char from head msgid='%s'", msgid)
		}
		line = strings.Replace(line, "\x00", " ", -1)
	}

	if strings.Contains(line, "\010") {
		if debug {
			log.Printf("Removed bkspc char from head msgid='%s'", msgid)
		}
		line = strings.Replace(line, "\010", " ", -1)
	}

	if strings.Contains(line, "\t") {
		if debug {
			log.Printf("Removed tab char from head msgid='%s'", msgid)
		}
		line = strings.Replace(line, "\t", " ", -1)
	}

	if strings.Contains(line, "\n") {
		if debug {
			log.Printf("Removed newline char from head msgid='%s'", msgid)
		}
		line = strings.Replace(line, "\n", " ", -1)
	}

	line = strings.Replace(line, "  ", " ", -1)
	return line
} // end func Clean_Headline

func Cleanup_NewsGroups_String(newsgroup string) string {
	if !utils.Line_isPrintable(newsgroup) {
		log.Printf("ERROR OV C_N_S unprintable newsgroup x='%x'", newsgroup)
		return ""
	}
	newsgroup = strings.TrimSpace(newsgroup)
	newsgroup = strings.ToLower(newsgroup)
	newsgroup = strings.Replace(newsgroup, "!", "", -1)   // clear char !
	newsgroup = strings.Replace(newsgroup, ">", "", -1)   // clear char !
	newsgroup = strings.Replace(newsgroup, "<", "", -1)   // clear char !
	newsgroup = strings.Replace(newsgroup, "*", "", -1)   // clear char !
	newsgroup = strings.Replace(newsgroup, "; ", " ", -1) // replace char " with .
	newsgroup = strings.Replace(newsgroup, ";", " ", -1)  // replace char " with .
	newsgroup = strings.Replace(newsgroup, "\"", "", -1)  // replace char " with .
	//newsgroup = strings.Replace(newsgroup, "/", ".", -1) // replace char / with .

	imat1, stopat1 := 0, 10
	for {
		if !strings.Contains(newsgroup, "..") {
			break
		}
		newsgroup = strings.Replace(newsgroup, "..", ".", -1)
		imat1++
		if imat1 >= stopat1 {
			log.Printf("ERROR Cleanup_NewsGroups_String imat >= stopat #1")
			return ""
		}
	}

	if newsgroup == "" {
		return ""
	}

	first_char := string(newsgroup[0])
	if !strings.Contains(CHECK_CHARS_LC, first_char) {
		return ""
	}

	imat2, stopat2 := 0, 10
	for {
		if newsgroup != "" &&
			(newsgroup[len(newsgroup)-1] == '.' || // clean some trailing chars
				newsgroup[len(newsgroup)-1] == '/' ||
				newsgroup[len(newsgroup)-1] == '\\' ||
				newsgroup[len(newsgroup)-1] == ',' ||
				newsgroup[len(newsgroup)-1] == ';') {
			// remove trailing ; sad client
			newsgroup = newsgroup[:len(newsgroup)-1]
		} else {
			break
		}
		imat2++
		if imat2 >= stopat2 {
			log.Printf("ERROR Cleanup_NewsGroups_String imat >= stopat #2")
			return ""
		}
	}

	if newsgroup == "" {
		return ""
	}

	/*
	// checking newsgroup compontents
	components := strings.Split(newsgroup, ".")
	for _, comp := range components {
		if utils.IsDigit(comp) {
			// component must not contain only numbers, causes issues with inn2 tradspool?storage??
			// why is this our problem? our spool doesn't have this problem.
			return ""
		}
	}
	*/

	/*
		forever_chars:
		for {
			check:
			for _, r := range check_chars {
				if len(newsgroup) == 0 {
					break forever_chars
				}
				if newsgroup[0] == r {
					continue check
				}
				// char is not valid, remove it
			} // end for every char
		}
	*/
	/*
		for {
			if !strings.HasPrefix(newsgroup, ".") && !strings.HasPrefix(newsgroup, "!") {
				break
			}
			newsgroup = newsgroup[0:]
			imat1++
			if imat1 >= stopat1 {
				log.Printf("ERROR Cleanup_NewsGroups_String imat >= stopat #1")
				return ""
			}
		}
	*/
	if newsgroup != "" {
		if !IsValidGroupName(newsgroup) {
			log.Printf("ERROR Cleanup_NewsGroups_String -> !IsValidGroupName('%s')", newsgroup)
			newsgroup = ""
		}
	}
	return newsgroup
} // end func Cleanup_NewsGroups_String

func Split_NewsGroups(msgid string, newsgroups_str string) []string {
	var newsgroups []string
	var newsgroups_dirty []string
	newsgroups_str = strings.Replace(newsgroups_str, ";", ",", -1)
	newsgroups_str = strings.Replace(newsgroups_str, ",,", ",", -1)
	newsgroups_str = strings.TrimSpace(newsgroups_str)

	has_spaces := strings.Contains(newsgroups_str, " ")
	has_commas := strings.Contains(newsgroups_str, ",")

	if has_commas && !has_spaces {
		newsgroups_dirty = strings.Split(newsgroups_str, ",")
	} else if !has_commas && has_spaces {
		newsgroups_dirty = strings.Split(newsgroups_str, " ")
	} else if !has_commas && !has_spaces {
		//newsgroups_dirty = strings.Split(newsgroups_str, " ")
		newsgroups_dirty = strings.Split(newsgroups_str, " ")
	} else if has_commas && has_spaces {
		//newsgroups_str = strings.Replace(newsgroups_str, ", ", ",", -1)
		//newsgroups_str = strings.Replace(newsgroups_str, " ,", ",", -1)
		newsgroups_dirty = strings.Split(newsgroups_str, ",")
		//log.Printf("ERROR Split_NewsGroups msgid='%s' has_commas=%t has_spaces=%t -> local.trash ng='%s'", msgid, has_commas, has_spaces, newsgroups_str)
		//return nil
		//return []string{ "local.trash" }
	}

	len_dirty := len(newsgroups_dirty)
	uniq_newsgroups := make(map[string]bool)

	e, o := 0, 0
	for i, newsgroup := range newsgroups_dirty {
		if newsgroup == "" {
			e++
			continue
		}
		if o >= LIMIT_SPLITMAX_NEWSGROUPS || e >= LIMIT_SPLITMAX_NEWSGROUPS {
			if DEBUG_OV {
				log.Printf("INFO BREAK Split_NewsGroups msgid='%s' o=%d e=%d len_dirty=%d", msgid, o, e, len_dirty)
			}
			break
		}

		newnewsgroup := Cleanup_NewsGroups_String(newsgroup)
		if newnewsgroup == "" {
			log.Printf("ERROR Cleanup_NewsGroups_String returned empty string msgid='%s' ngs='%s' i=%d o=%d e=%d len_dirty=%d !uniq=%t", msgid, newsgroup, i, o, e, len_dirty, uniq_newsgroups[newsgroup])
			e++
			continue
		}
		if newnewsgroup != newsgroup {
			newsgroup = newnewsgroup
		}

		if !uniq_newsgroups[newsgroup] {
			uniq_newsgroups[newsgroup] = true
			newsgroups = append(newsgroups, newsgroup)
			o++

		} else {
			//log.Printf("ERROR Split_NewsGroups msgid='%s' group='%s' i=%d o=%d e=%d len_dirty=%d !uniq=%t", msgid, newsgroup, i, o, e, len_dirty, uniq_newsgroups[newsgroup])
			e++
		}

	} // end for newsgroup

	if len(newsgroups) == 0 {
		//newsgroup = "local.trash"
		log.Printf("ERROR Split_NewsGroups msgid='%s' input='%s' returned=%d o=%d e=%d len_dirty=%d > local.trash", msgid, newsgroups_str, len(newsgroups), o, e, len_dirty)
	}
	return newsgroups
} // end func Split_NewsGroups

func IsValidGroupName(group string) bool {
	if group == "" {
		log.Printf("!IsValidGroupName group='%s' empty", group)
		return false
	}

	// first char of group has to be a letter or digit and not uppercase
	for i, r := range group[:1] {

		if unicode.IsUpper(r) {
			log.Printf("!IsValidGroupName IsUpper i=%d group='%s'", i, group)
			return false
		}

		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			log.Printf("!IsValidGroupName !IsLetter !IsDigit i=%d group='%s'", i, group)
			return false
		}
	}

	if len(group) > 1 {
	loop_check_chars:
		for i, r := range group[1:] { // check chars without first char, we checked it before
			valid := false
			// check if this char is not Uppercased and is unicode letter or digit
			if !unicode.IsUpper(r) && (unicode.IsLetter(r) || unicode.IsDigit(r)) {
				valid = true
			} else if !unicode.IsUpper(r) {
				// is not letter or digit, check more chars
				// these are valid for groups
				switch string(r) {
				case ".":
					valid = true
				case "_":
					valid = true
				case "-":
					valid = true
				case "+":
					valid = true
				case "&":
					valid = true
				}
			}
			if valid {
				continue loop_check_chars
			}
			log.Printf("!IsValidGroupName #3 group='%s' i=%d r=x'%x'", group, i, string(r))
			return false
			/*
				if valid {
					if string(r) == prev_char {
						if cont >= 2 {
							// dont allow more than two of these in a row: group "c++" is ok, c+++ is not
							log.Printf("!IsValidGroupName group='%s' cont=%d i=%d r='%s'", group, cont, i, string(r))
							return false
						}
					} else {
						cont = 1
					}
					prev_char = string(r)
					continue loop_check_chars
				}
				log.Printf("!IsValidGroupName #3 group='%s' i=%d r='%s'", group, i, string(r))
				return false
			*/

		} // end for range group[1:]
	}
	return true
} // end func IsValidGroupName

func Test_Overview(file_path string, DEBUG bool) bool {
	// test should never try to update the footer!
	update_footer := false
	if ovfh, err := Open_ov(file_path); err != nil {
		log.Printf("ERROR OV TEST_Overview Open_ov err='%v' fp='%s'", err, filepath.Base(file_path))
		return false

	} else {
		if err := Close_ov(ovfh, update_footer, true); err != nil {
			log.Printf("ERROR OV TEST_Overview Close_ov err='%v' fp='%s'", err, filepath.Base(file_path))
			return false
		}
	}
	if DEBUG {
		log.Printf("OK Test_Overview fp='%s'", file_path)
	}
	return true
} // end func Test_Overview

func get_hash_from_filename(file_path string) (string, error) {
	file := filepath.Base(file_path)
	if !strings.HasSuffix(file, ".overview") {
		return "", fmt.Errorf("ERROR get_hash_from_file file_path='%s' !HasSuffix .overview", file_path)
	}

	hash := strings.Split(file, ".")[0]
	if len(hash) < 32 {
		return "", fmt.Errorf("ERROR Open_ov len(hash) < 32 fp='%s'", filepath.Base(file_path))
	}
	return hash, nil
}

func Open_ov(file_path string) (*OVFH, error) {
	var err error
	var hash string
	if hash, err = get_hash_from_filename(file_path); err != nil {
		err = fmt.Errorf("ERROR Open_ov -> hash err='%v' fp='%s'", err, filepath.Base(file_path))
		return nil, err
	}

	// pass request to handler channel and wait on our supplied reply_channel

	// create open_request
	var open_request Overview_Open_Request
	reply_chan := make(chan Overview_Reply, 1) // FIXME: mark*836b04be* can we not create the reply_chan everytime? just pass it from upper end to here and reuse it?
	open_request.reply_chan = reply_chan
	open_request.file_path = file_path
	open_request.hash = hash

	// pass open_request to open_request_chan
	open_request_chan <- open_request

	// wait for reply
	if DEBUG_OV {
		log.Printf("WAITING Open_ov -> reply_chan fp='%s'", filepath.Base(file_path))
	}
	reply := <-reply_chan
	// got reply
	if DEBUG_OV {
		log.Printf("GOT REPLY Open_ov -> reply_chan fp='%s'", filepath.Base(file_path))
	}
	//globalCounter.Dec("wait_open_request")

	if reply.err == nil {

		if DEBUG_OV {
			log.Printf("OK REPLY Open_ov -> fp='%s'", filepath.Base(file_path))
		}
		return reply.ovfh, nil

	} else {
		err = reply.err
		log.Printf("ERROR REPLY Open_ov -> reply_chan err='%v' fp='%s'", err, filepath.Base(file_path))
	}

	return nil, fmt.Errorf("ERROR Open_ov final err='%v' fp='%s'", err, filepath.Base(file_path))
} // end func Open_ov

func handle_open_ov(hash string, file_path string) (*OVFH, error) {
	var err error
	var file_handle *os.File
	var mmap_handle mmap.MMap
	var ov_footer string
	cs := 0

	if !utils.FileExists(file_path) {
		return nil, fmt.Errorf("ERROR Open_ov !fileExists fp='%s'", file_path)
	}

	if file_handle, err = os.OpenFile(file_path, os.O_RDWR, 0644); err != nil {
		log.Printf("ERROR Open_ov -> mmap.Map err='%v' cs=%d fp='%s'", err, cs, file_path)
		return nil, err
	}
	cs++ // 1

	if mmap_handle, err = mmap.Map(file_handle, mmap.RDWR, 0); err != nil {
		log.Printf("ERROR Open_ov -> Read_Head_ov err='%v' cs=%d fp='%s'", err, cs, file_path)
		return nil, err
	}
	cs++ // 2

	time_open, time_flush, written := utils.Now(), utils.Now(), 0

	ovfh := &OVFH{} // { file_path, file_handle, mmap_handle, mmap_size, time_open, time_flush, written, 0, 0 }
	ovfh.File_path = file_path
	ovfh.File_handle = file_handle
	ovfh.Mmap_handle = mmap_handle
	ovfh.Mmap_size = len(ovfh.Mmap_handle)
	ovfh.Mmap_range = ovfh.Mmap_size - 1
	ovfh.Time_open = time_open
	ovfh.Time_flush = time_flush
	ovfh.Written = written
	ovfh.Hash = hash

	if _, err := Read_Head_ov(ovfh); err != nil { // _ = ov_header
		log.Printf("ERROR Open_ov -> Read_Head_ov err='%v' cs=%d fp='%s'", err, cs, file_path)
		return nil, err
	}
	cs++ // 3

	if ov_footer, err = Read_Foot_ov(ovfh); err != nil {
		log.Printf("ERROR Open_ov -> Read_Foot_ov err='%v' cs=%d fp='%s'", err, cs, file_path)
		return nil, err
	}
	cs++ // 4

	foot := strings.Split(ov_footer, ",")
	if len(foot) != SIZEOF_FOOT {
		log.Printf("ERROR Open_ov -> len(foot)=%d != SIZEOF_FOOT=%d fp='%s'", len(foot), SIZEOF_FOOT, file_path)
		return nil, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
	}
	cs++ // 5

	if !strings.HasPrefix(foot[1], "last=") ||
		!strings.HasPrefix(foot[2], "Findex=") ||
		!strings.HasPrefix(foot[3], "bodyend=") ||
		!strings.HasPrefix(foot[4], "fend=") {
		log.Printf("ERROR Open_ov -> error !HasPrefix foot fp='%s'", file_path)
		return nil, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
	}
	cs++ // 6

	last, findex := utils.Str2uint64(strings.Split(foot[1], "=")[1]), utils.Str2int(strings.Split(foot[2], "=")[1])
	bodyend, fend := utils.Str2int(strings.Split(foot[3], "=")[1]), utils.Str2int(strings.Split(foot[4], "=")[1])
	if findex >= bodyend && bodyend != fend-OV_RESERVE_END {
		log.Printf("ERROR Open_ov -> findex=%d > bodyend=%d ? fend=%d fp='%s'", findex, bodyend, fend, file_path)
		return nil, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
	}
	cs++ // 7

	if findex < OV_RESERVE_BEG || findex > OV_RESERVE_BEG && last == 0 {
		log.Printf("ERROR Open_ov -> findex=%d OV_RESERVE_BEG=%d last=%d fp='%s'", findex, OV_RESERVE_BEG, last, file_path)
		return nil, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
	}
	cs++ // 8

	ovfh.Findex = findex
	ovfh.Last = last
	if retbool, ovfh := Replay_Footer(ovfh); retbool == true {
		return ovfh, nil
	} else {
		log.Printf("ERROR Open_ov -> Replay_Footer fp='%s' retbool=%t", file_path, retbool)

	}

	return nil, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
} // end func handle_open_ov

func Close_ov(ovfh *OVFH, update_footer bool, force_close bool) error {
	var err error

	file := filepath.Base(ovfh.File_path)
	var close_request Overview_Close_Request
	reply_chan := make(chan Overview_Reply, 1) // FIXME: mark*836b04be* can we not create the reply_chan everytime? just pass it from upper end to here and reuse it?
	close_request.ovfh = ovfh
	close_request.reply_chan = reply_chan
	if force_close {
		close_request.force_close = true
	}
	//globalCounter.Inc("wait_close_request")
	close_request_chan <- close_request
	// wait for reply
	if DEBUG_OV {
		log.Printf("WAITING Close_ov -> reply_chan fp='%s'", file)
	}
	reply := <-reply_chan
	// got reply
	if DEBUG_OV {
		log.Printf("GOT REPLY Close_ov -> reply_chan fp='%s'", file)
	}
	//globalCounter.Dec("wait_close_request")
	if reply.err == nil {
		if DEBUG_OV {
			log.Printf("OK REPLY Close_ov -> fp='%s'", file)
		}
	} else {
		err = reply.err
		log.Printf("ERROR REPLY Close_ov -> reply_chan err='%v' fp='%s'", err, file)
	}

	return err
} // end func Close_ov

func handle_close_ov(ovfh *OVFH, update_footer bool, force_close bool, grow bool) error {
	var err error
	if update_footer {
		if err := Update_Footer(ovfh); err != nil {
			return err
		}
	}

	if err = ovfh.Mmap_handle.Flush(); err == nil {
		if err = ovfh.Mmap_handle.Unmap(); err == nil {
			if err = ovfh.File_handle.Close(); err == nil {
				if DEBUG_OV {
					log.Printf("Close_ov update_footer=%t grow=%t OK fp='%s'", update_footer, grow, ovfh.File_path)
				}
				return nil
			}
		}
	}
	log.Printf("ERROR Close_ov fp='%s' err='%v'", ovfh.File_path, err)
	return err
} // end func handle_close_ov

func Flush_ov(ovfh *OVFH) error {
	var err error
	if err = ovfh.Mmap_handle.Flush(); err != nil {
		log.Printf("ERROR Flush_ov fp='%s' err='%v'", ovfh.File_path, err)
	} else {
		if DEBUG_OV {
			log.Printf("Flush_ov OK fp='%s'", ovfh.File_path)
		}
	}
	return err
} // end func Flush_ov

func Replay_Footer(ovfh *OVFH) (bool, *OVFH) {

	if ovfh.Last == 0 && ovfh.Findex == OV_RESERVE_BEG {
		if DEBUG_OV {
			log.Printf("Replay_Footer NEW OK ovfh.Findex=%d", ovfh.Findex)
		}
		return true, ovfh
	}
	position, frees, newlines := 1, 0, 0
	needs_trues := 2
	startindex, endindex := ovfh.Findex, 0
	tabs, trues := 0, 0

replay:
	for {
		c := ovfh.Mmap_handle[startindex] // check char at index
		if DEBUG_OV {
			log.Printf("DEBUG REPLAY: c='%x' startindex=%d", c, startindex)
		}

		switch c {
		case 0:
			// char is a <nul> and has to be at 1st position
			if position == 1 {
				//trues++
			} else {
				log.Printf("ERROR OV c=nul position=%d startindex=%d endindex=%d Findex=%d fp='%s'", position, startindex, endindex, ovfh.Findex, ovfh.File_path)
			}
			frees++ // should only be 1 free byte, always
			if frees > 1 {
				log.Printf("ERROR OV frees=%d > 1 position=%d startindex=%d endindex=%d Findex=%d fp='%s'", frees, position, startindex, endindex, ovfh.Findex, ovfh.File_path)
			}
		case '\t':
			// char is a <tab>
			tabs++
			if tabs > OVERVIEW_TABS {
				log.Printf("ERROR OV tabs=%d > %d position=%d startindex=%d fp='%s'", tabs, OVERVIEW_TABS, position, startindex, ovfh.File_path)
			}
		case '\n':
			// char is a <newline>
			newlines++
			switch newlines {
			case 1:
				// found first newline
				if position == 2 || position-frees == 1 {
					// first newline has to be at 2nd replay position
					// or if there are more free spaces because we manipulated a broken last entry
					// calculate the pos, should then result in 1
					trues++
				} else {
					space := position - frees
					log.Printf("ERROR OV 1st newline position=%d != 2 space=%d fp='%s'", position, space, ovfh.File_path)
				}
				endindex = startindex
			case 2:
				// found second newline, in between we should have 'n' tabs with the overview fields
				if tabs == OVERVIEW_TABS {
					trues++
				}
				break replay
			} // end switch newlines
		} // end switch c

		if newlines == 2 {
			log.Printf("ERROR Replay Footer found 2 newlines but failed to confirm content?! fp='%s'", ovfh.File_path)
			// anyways found 2 newlines, break and see checks
			break
		}

		if startindex < OV_RESERVE_BEG {
			// dont run into the header...
			log.Printf("ERROR Replay Footer startindex=%d < OV_RESERVE_BEG=%d fp='%s'", startindex, OV_RESERVE_BEG, ovfh.File_path)
			break
		}
		startindex-- // decrease index to walk backwards
		position++   // count position upwards

	} // end for reverse search

	retstring := fmt.Sprintf("needs_trues=%d trues=%d frees=%d newlines=%d tabs=%d position=%d startindex=%d endindex=%d fp='%s'",
		needs_trues, trues, frees, newlines, tabs, position, startindex, endindex, ovfh.File_path)

	if trues != needs_trues {
		log.Printf("ERROR Replay Footer %s", retstring)
		return false, ovfh
	}

	if tabs != OVERVIEW_TABS {
		log.Printf("ERROR Replay_Footer lastline startindex=%d tabs=%d != OVERVIEW_TABS=%d", startindex, tabs, OVERVIEW_TABS)
		return false, ovfh
	}

	if endindex == 0 || endindex < startindex {
		log.Printf("ERROR Replay_Footer endindex=%d < startindex=%d", endindex, startindex)
		return false, ovfh
	}

	if DEBUG_OV {
		log.Printf("OK Replay Footer %s", retstring)
	}

	//log.Printf("Replay_Footer PRE tabs=%d newlines=%d frees=%d", tabs, newlines, frees)

	// read the last line and get the first field with last written article number to file,
	// which should be the ovh.Last value - 1
	lastline := string(ovfh.Mmap_handle[startindex+1 : endindex])
	if DEBUG_OV {
		log.Printf("Replay_Footer lastline='%s'", lastline)
	}
	last_str := strings.Split(lastline, "\t")[0]
	if frees > 1 {
		oldFindex := ovfh.Findex
		diff := frees - 1
		ovfh.Findex -= diff
		log.Printf("WARN OV adjusted frees=%d oldFindex=%d - diff=%d = Findex=%d", frees, oldFindex, diff, ovfh.Findex)
		time.Sleep(5 * time.Second) // DEBUG SLEEP

	}
	if utils.IsDigit(last_str) {
		if utils.Str2uint64(last_str) == ovfh.Last-1 {
			if DEBUG_OV {
				log.Printf("Replay_Footer OK fp='%s' last=%s next=%d", ovfh.File_path, last_str, ovfh.Last)
			}
			return true, ovfh
		}
	}
	log.Printf("ERROR Replay_Footer last=%s ovfh.Last=%d  fp='%s'", last_str, ovfh.Last, ovfh.File_path)
	return false, ovfh
} // end Replay_Footer

func Write_ov(ovfh *OVFH, data string, is_head bool, is_foot bool, grow bool) (error, string) {
	var err error
	//len_data := len(data)
	databyte := []byte(data)
	len_data := len(databyte)
	mmap_size := len(ovfh.Mmap_handle)
	if DEBUG_OV {
		log.Printf("Write_ov len_data=%d is_head=%t is_foot=%t", len_data, is_head, is_foot)
	}

	// set start Findex vs reserved space at beginning of map
	if !is_head && !is_foot && ovfh.Findex < OV_RESERVE_BEG {
		ovfh.Findex = OV_RESERVE_BEG
	}

	if mmap_size != ovfh.Mmap_size { // unsure if this could change while mapped we have serious trouble
		return fmt.Errorf("ERROR Write_ov len(ovfh.Mmap_handle)=%d != ovfh.Mmap_size=%d fp='%s'", mmap_size, ovfh.Mmap_size, ovfh.File_path), ""
	}

	if ovfh.Mmap_range < OV_RESERVE_BEG+OV_RESERVE_END {
		return fmt.Errorf("ERROR Write_ov ovfh.Mmap_size=%d fp='%s'", ovfh.Mmap_size, ovfh.File_path), ""
	}

	// total bodyspace of overview file
	bodyspace := ovfh.Mmap_size - OV_RESERVE_BEG - OV_RESERVE_END
	if bodyspace <= 0 {
		return fmt.Errorf("ERROR Write_ov bodyspace=%d fp='%s'", bodyspace, ovfh.File_path), ""
	}

	// dont count OV_RESERVE_BEG here as it is already included in Findex
	bodyend := ovfh.Mmap_range - OV_RESERVE_END
	freespace := bodyend - ovfh.Findex
	// newbodysize adds len of data to Findex
	// then newbodysize should not be higher than freespace
	newbodysize := len_data + ovfh.Findex

	if DEBUG_OV {
		log.Printf("Write_ov bodyspace=%d len_data=%d freespace=%d Findex=%d newsize=%d", len_data, bodyspace, freespace, ovfh.Findex, newbodysize)
	}

	if !is_foot && (freespace <= 1 || newbodysize >= bodyend) {
		if DEBUG_OV {
			log.Printf("GROW OVERVIEW Findex=%d len_data=%d freespace=%d bodyend=%d newsize=%d hash='%s'", ovfh.Findex, len_data, freespace, bodyend, newbodysize, ovfh.Hash)
		}

		if err = Grow_ov(ovfh, 1, "128K", 0); err != nil {
			overflow_err := fmt.Errorf("ERROR Write_ovfh -> Grow_ov err='%v' newsize=%d avail=%d mmap_size=%d", err, newbodysize, freespace, ovfh.Mmap_size)
			return overflow_err, ERR_OV_OVERFLOW
		}
		if DEBUG_OV {
			log.Printf("DONE GROW OVERVIEW hash='%s'", ovfh.Hash)
		}
	}

	if is_foot && data == "" && grow == true {
		// zerofill-overwrite the footer space
		index := ovfh.Mmap_size - OV_RESERVE_END
		databyte := []byte(zerofill(ZERO_PATTERN, OV_RESERVE_END))
		// writes data to mmap byte for byte
		for pos, abyte := range databyte {
			if index >= ovfh.Mmap_size {
				if DEBUG_OV {
					log.Printf("Write_ov GROW fp='%s' reached end index=%d mmap_size=%d pos=%d len_databyte=%d break", ovfh.File_path, index, ovfh.Mmap_size, pos+1, len(databyte))
				}
				break
			}
			ovfh.Mmap_handle[index] = abyte
			index++
		}

	} else if is_foot && data != "" { // footer data is not empty, write footer
		startindex := ovfh.Mmap_size - OV_RESERVE_END
		// writes data to mmap byte for byte
		for _, abyte := range databyte {
			ovfh.Mmap_handle[startindex] = abyte
			startindex++
		}
		ovfh.Written += len_data

	} else if !is_head && !is_foot && data != "" { // write data

		startindex := ovfh.Findex
		limit := ovfh.Mmap_range - OV_RESERVE_END
		if ovfh.Mmap_handle == nil {
			return fmt.Errorf("ERROR overview.Write_ovfh Mmap_handle == nil fp='%s'", ovfh.File_path), ""
		}
		if DEBUG_OV {
			log.Printf("Write_ov data=%d Findex=%d limit=%d range=%d handle=%d fp='%s'", len(data), startindex, limit, ovfh.Mmap_range, len(ovfh.Mmap_handle), ovfh.File_path)
		}

		// writes data to mmap byte for byte
		for _, abyte := range databyte {
			if ovfh.Findex >= limit {
				break
			}
			ovfh.Mmap_handle[ovfh.Findex] = abyte
			ovfh.Findex++
		}
		ovfh.Written += len_data

	} // !is_head && ! is_foot
	return nil, ""
} // end func Write_ov

func Read_Head_ov(ovfh *OVFH) (string, error) {
	if ovfh.Mmap_size > OV_RESERVE_BEG {
		ov_header := string(ovfh.Mmap_handle[0:OV_RESERVE_BEG])
		if check_ovfh_header(ov_header) {
			return ov_header, nil
		} else {
			return "", fmt.Errorf("ERROR Read_Head_ov -> check_ovfh_header'")
		}
	}
	return "", fmt.Errorf("ERROR Read_Head_ov 'mmap_size=%d < OV_RESERVE_BEG=%d'", ovfh.Mmap_size, OV_RESERVE_BEG)
} // end func Read_Head_ov

func Read_Foot_ov(ovfh *OVFH) (string, error) {
	foot_start := ovfh.Mmap_size - OV_RESERVE_END
	if foot_start > OV_RESERVE_END {
		ov_footer := string(ovfh.Mmap_handle[foot_start:])
		if check_ovfh_footer(ov_footer) {
			if DEBUG_OV {
				log.Printf("OK Read_Foot_ov -> check_ovfh_footer")
			}
			return ov_footer, nil
		} else {
			return "", fmt.Errorf("ERROR Read_Foot_ov -> check_ovfh_footer")
		}
	}
	return "", fmt.Errorf("ERROR Read_Foot_ov mmap_size=%d 'foot_start=%d < OV_RESERVE_END=%d'", ovfh.Mmap_size, foot_start, OV_RESERVE_END)
} // end func Read_Foot_ov

func Create_ov(File_path string, hash string, pages int) error {
	var err error
	if utils.FileExists(File_path) {
		return fmt.Errorf("ERROR Create_ov exists fp='%s'", File_path)
	}
	bytesize := pages * 1024
	now := utils.Now()
	head_str := fmt.Sprintf("%s%d,group=%s,zeropad=%s,%s", HEADER_BEG, now, hash, ZERO_PATTERN, HEADER_END)
	bodyend := OV_RESERVE_BEG + bytesize
	foot_str := fmt.Sprintf("%s%d,last=0,Findex=%d,bodyend=%d,fend=%d,zeropad=%s,%s", FOOTER_BEG, now, OV_RESERVE_BEG, bodyend, bodyend+OV_RESERVE_END, ZERO_PATTERN, FOOTER_END)
	ov_header := zerofill(head_str, OV_RESERVE_BEG)
	ov_body := zerofill_block(pages, "1K") // initial max of overview data, will grow when needed
	ov_footer := zerofill(foot_str, OV_RESERVE_END)

	wb, wbt := 0, 0 // debugs written bytes

	if wb, err = init_file(File_path, ov_header, false); err != nil {
		log.Printf("ERROR Create_ov init_file ov_header fp='%s' err='%v'", File_path, err)
		return err
	}
	wbt += wb

	if wb, err = init_file(File_path, ov_body, false); err != nil {
		log.Printf("ERROR Create_ov init_file ov_body fp='%s' err='%v'", File_path, err)
		return err
	}
	wbt += wb

	if wb, err = init_file(File_path, ov_footer, false); err != nil {
		log.Printf("ERROR Create_ov init_file ov_footer fp='%s' err='%v'", File_path, err)
		return err
	}
	wbt += wb

	if DEBUG_OV { log.Printf("Create_ov OK fp='%s' wbt=%d", File_path, wbt) }
	return nil

} // end func Create_ov

func Grow_ov(ovfh *OVFH, pages int, blocksize string, mode int) (error) {
	var err error
	var errstr string
	var header string
	var footer string
	var wbt int

	if DEBUG_OV {
		log.Printf("Grow_ov pages=%d bs=%s fp='%s'", pages, blocksize, ovfh.File_path)
	}

	if mode != 999 { // dont do these checks if we want to fix overview footer

		// update footer
		if err := Update_Footer(ovfh); err != nil {
			return err
		}

		// check header
		if header, err = Read_Head_ov(ovfh); err != nil {
			return err
		}
		if retbool := check_ovfh_header(header); !retbool {
			return fmt.Errorf("ERROR Grow_ov -> check_ovfh_header fp='%s' header='%s' retbool=false", ovfh.File_path, header)
		}
		if DEBUG_OV {
			log.Printf("Grow_ov fp='%s' check_ovfh_header OK Findex=%d", ovfh.File_path, ovfh.Findex)
		}

		// check footer
		if footer, err = Read_Foot_ov(ovfh); err != nil {
			return err
		}
		if retbool := check_ovfh_footer(footer); !retbool {
			return fmt.Errorf("ERROR Grow_ov -> check_ovfh_footer fp='%s' footer='%s' retbool=false", ovfh.File_path, footer)
		}
		if DEBUG_OV {
			log.Printf("Grow_ov fp='%s' check_ovfh_footer OK Findex=%d", ovfh.File_path, ovfh.Findex)
		}

		// 1. overwrite footer area while still mapped
		if err, errstr = Write_ov(ovfh, "", false, true, true); err != nil {
			log.Printf("ERROR Grow_ov -> Write_ov err='%v' errstr='%s'", err, errstr)
			return err
		}

	} // end if mode != 999

	// 2. unmap and close overview mmap
	//force_close := true
	//if err = Close_ov(ovfh, false, force_close); err != nil {
	if err = handle_close_ov(ovfh, false, false, true); err != nil {
		return err
	}
	if DEBUG_OV {
		log.Printf("Grow_ov fp='%s' mmap closed OK", ovfh.File_path)
	}

	// 3. extend the overview body
	if wb, err := init_file(ovfh.File_path, zerofill_block(pages, blocksize), true); err != nil {
		log.Printf("ERROR Grow_ov -> init_file err='%v'", err)
		return err
	} else {
		wbt += wb
	}
	if DEBUG_OV {
		log.Printf("Grow_ov fp='%s' zerofill_block=%d OK", ovfh.File_path, wbt)
	}

	// 4. append footer
	ov_footer := construct_footer(ovfh)
	if wb, err := init_file(ovfh.File_path, ov_footer, true); err != nil {
		log.Printf("ERROR Grow_ov -> init_file2 err='%v'", err)
		return err
	} else {
		wbt += wb
	}
	// footer appended

	// 5. reopen mmap file
	//if ovfh, err = Open_ov(ovfh.File_path); err != nil {
	if ovfh, err = handle_open_ov(ovfh.Hash, ovfh.File_path); err != nil {
		log.Printf("ERROR Grow_ov -> Open_ov err='%v'", err)
		return err
	}

	// 6. done
	body_end := ovfh.Mmap_size - OV_RESERVE_END
	if DEBUG_OV {
		log.Printf("Grow_ov OK fp='%s' wbt=%d body_end=%d Findex=%d", ovfh.File_path, wbt, body_end, ovfh.Findex)
	}
	return nil
} // end func Grow_ov

func Update_Footer(ovfh *OVFH) (error) {
	if ovfh.Findex == 0 || ovfh.Last == 0 {
		return fmt.Errorf("ERROR Update_Footer ovfh.Findex=%d ovfh.Last=%d", ovfh.Findex, ovfh.Last)
	}
	var err error
	//var errstr string
	ov_footer := construct_footer(ovfh)
	err, _ = Write_ov(ovfh, ov_footer, false, true, false)
	if err != nil {
		log.Printf("ERROR Update_Footer -> Write_ov err='%v'", err)
	} else {
		if DEBUG_OV {
			log.Printf("OK Update_Footer -> Write_ov len_ov_footer=%d", len(ov_footer))
		}
	}
	return err
} // end func Update_Footer

// private overview functions

func construct_footer(ovfh *OVFH) string {
	bodyend := ovfh.Mmap_size - OV_RESERVE_END
	foot_str := fmt.Sprintf("%s%d,last=%d,Findex=%d,bodyend=%d,fend=%d,zeropad=%s,%s", FOOTER_BEG, utils.Nano(), ovfh.Last, ovfh.Findex, bodyend, bodyend+OV_RESERVE_END, ZERO_PATTERN, FOOTER_END)
	ov_footer := zerofill(foot_str, OV_RESERVE_END)
	return ov_footer
} // end func construct_footer

func check_ovfh_header(header string) bool {
	if strings.HasPrefix(header, HEADER_BEG) {
		if strings.HasSuffix(header, HEADER_END) {
			return true
		} else {
			log.Printf("ERROR check_ovfh_header !HasSuffix")
		}
	} else {
		log.Printf("ERROR check_ovfh_header !HasPrefix")
	}
	return false
} // end func check_ovfh_header

func check_ovfh_footer(footer string) bool {
	if strings.HasPrefix(footer, FOOTER_BEG) {
		if strings.HasSuffix(footer, ","+FOOTER_END) {
			/*
				foot := strings.Split(footer, ",")
				if len(foot) == SIZEOF_FOOT {
					if strings.HasPrefix(foot[1], "last=") && strings.HasPrefix(foot[2], "Findex=") {
						last_str, findex_str := strings.Split(foot[1], "=")[1], strings.Split(foot[2], "=")[1]
						if isDigit(last_str) && isDigit(findex_str) {
							last, findex := utils.Str2int(last_str), utils.Str2int(findex_str)
							if findex > 0 && last >= 0 {
								ovfh.Findex = findex
								ovfh.Last = last
								return true, ovfh
							}
						}
					}
				}
			*/
			return true
		} else {
			log.Printf("ERROR check_ovfh_footer !HasSuffix")
		}

	} else {
		log.Printf("ERROR check_ovfh_footer !HasPrefix")
	}
	return false
} // end func check_ovfh_footer

func init_file(File_path string, data string, grow bool) (int, error) {
	if DEBUG_OV {
		log.Printf("init_file fp='%s' len_data=%d grow=%t", File_path, len(data), grow)
	}
	var fh *os.File
	var err error
	var wb int
	if fh, err = os.OpenFile(File_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		defer fh.Close()
		w := bufio.NewWriter(fh)
		if wb, err = w.WriteString(data); err == nil {
			if err = w.Flush(); err == nil {
				if DEBUG_OV {
					log.Printf("init_file wrote fp='%s' len_data=%d wb=%d grow=%t", File_path, len(data), wb, grow)
				}
				return wb, nil
			}
		}
	}
	log.Printf("ERROR init_file err='%v'", err)
	return wb, err
} // end func init_file

func preload_zero(what string) {
	if what == "PRELOAD_ZERO_1K" && PRELOAD_ZERO_1K == "" {
		PRELOAD_ZERO_1K = zerofill(ZERO_PATTERN, 1024)
		log.Printf("OV PRELOAD_ZERO_1K=%d", len(PRELOAD_ZERO_1K))
	} else if what == "PRELOAD_ZERO_4K" && PRELOAD_ZERO_4K == "" {
		PRELOAD_ZERO_4K = zerofill_block(4, "1K")
		log.Printf("OV PRELOAD_ZERO_4K=%d", len(PRELOAD_ZERO_4K))
	} else if what == "PRELOAD_ZERO_128K" && PRELOAD_ZERO_128K == "" {
		PRELOAD_ZERO_128K = zerofill_block(32, "4K")
		log.Printf("OV PRELOAD_ZERO_128K=%d", len(PRELOAD_ZERO_128K))
	} else if what == "PRELOAD_ZERO_1M" && PRELOAD_ZERO_1M == "" {
		PRELOAD_ZERO_1M = zerofill_block(8, "128K")
		log.Printf("OV PRELOAD_ZERO_1M=%d", len(PRELOAD_ZERO_1M))
	}
} // end func preload_zero

func zerofill(astring string, fillto int) string {
	//log.Printf("zerofill len_astr=%d fillto=%d", len(astring), fillto)
	strlen := len(astring) - ZERO_PATTERN_LEN
	diff := fillto - strlen
	zf, i := "", 0
	for j := i; j < diff; j++ {
		zf += ZERO_FILL_STR
	}
	retstring := strings.Replace(astring, ZERO_PATTERN, zf, 1)
	if DEBUG_OV {
		log.Printf("zerofill astring='%s' input=%d diff=%d output=%d zf=%d", astring, strlen, diff, len(retstring), len(zf))
	}
	return retstring
} // end func zerofill

func zerofill_block(pages int, blocksize string) string {
	if DEBUG_OV {
		log.Printf("zerofill_block pages=%d blocksize=%s", pages, blocksize)
	}
	filler := ""
	switch blocksize {
	case "1K":
		filler = PRELOAD_ZERO_1K
	case "4K":
		filler = PRELOAD_ZERO_4K
	case "128K":
		filler = PRELOAD_ZERO_128K
	case "1M":
		filler = PRELOAD_ZERO_1M
	}
	zf, i := "", 0
	for j := i; j < pages; j++ {
		zf += filler
	}
	return zf
} // end func zerofill_block

func is_closed_server(stop_server_chan chan bool) bool {
	isclosed := false

	// try reading a true from channel
	select {
	case has_closed, ok := <-stop_server_chan:
		// ok will be !ok if channel is closed!
		if !ok || has_closed {
			isclosed = true
		}
	default:
		// if nothing in, defaults breaks select
		isclosed = false
	}

	return isclosed
} // end func is_closed_server

func isvalidmsgid(astring string, silent bool) bool {
	if !utils.IsDigit(astring) {
		//if strings.HasPrefix(astring, "<") && strings.Contains(astring, "@") && strings.HasSuffix(astring, ">") {
		if len(astring) >= 5 { // <a@a>
			if astring[0] == '<' && astring[len(astring)-1] == '>' {
				//log.Printf("isvalidmsgid(%s)==true", astring)
				return true
			}
		}
	} else {
		//log.Printf("msgid is digit and valid", astring)
		return true
	}
	if !silent || DEBUG_OV {
		log.Printf("ERROR !overview.isvalidmsgid('%s')", astring)
	}
	return false
} // end func isvalidmsgid

func Scan_Overview(file *string, a *uint64, b *uint64, fields *string, conn net.Conn, initline string, txb *int) ([]*string, error) {
    var lines []*string
    var sent uint64
    readFile, err := os.Open(*file)
    if err != nil {
        log.Printf("Error scan_overview err='%v'", err)
        return lines, err
    }
    defer readFile.Close()
    fileScanner := bufio.NewScanner(readFile)
    fileScanner.Split(bufio.ScanLines)
    lc := uint64(0) // linecounter
	if conn != nil {
		if initline == "" {
			initline = "224 xover follows"
		}
		// conn is set: send init line
		tx, err := io.WriteString(conn, initline+CRLF)
		if err != nil {
			return lines, err
		}
		*txb += tx
	}
    for fileScanner.Scan() {
        if lc < *a {
			// pass
        } else
        if lc >= *a && lc <= *b {
            line := fileScanner.Text()
            ll := len(line)
            if ll == 0 {
				break
			} else
            if ll > 0 && ll <= 3 {
				if line[0] == '\x00' || line == "EOV" {
					log.Printf("break Scan_Overview lc=%d")
					break
				}
			}
            if *fields == "all" {
				if conn == nil {
					lines = append(lines, &line)
				} else {
					// conn is set: send lines
					tx, err := io.WriteString(conn, line+CRLF)
					if err != nil {
						return lines, err
					}
					sent++
					*txb += tx
				}
            } else
            if *fields == "msgid" {
                fields := strings.Split(line, "\t")
                if len(fields) == OVERVIEW_FIELDS {
					if isvalidmsgid(fields[4], true) {
						//return &fields[4]
						lines = append(lines, &fields[4]) // catches message-id field

					} else {
						log.Printf("Error Scan_Overview file='%s' lc=%d field[4] err='!isvalidmsgid'", filepath.Base(*file), lc)
						break
					}
                } else {
					log.Printf("Error Scan_Overview file='%s' lc=%d", file, lc)
					break
				}
            } else {
				log.Printf("Error scan_overview unknown *fields=%s", *fields)
				break
			}
        }
        // finally break out
        if lc >= *b {
			// conn is set: send final dot
			if conn != nil {
				tx, err := io.WriteString(conn, "."+CRLF)
				if err != nil {
					return lines, err
				}
				*txb += tx
				log.Printf("Scan_Overview file='%s' a=%d b=%d sent=%d txbytes=%d", filepath.Base(*file), *a, *b, sent, *txb)
				return lines, nil
			}
            return lines, err
        }
        lc++
    } // end for filescanner
	log.Printf("Scan_Overview: file='%s' lc=%d", *file, lc)
    return lines, err
} // end func Scan_Overview
