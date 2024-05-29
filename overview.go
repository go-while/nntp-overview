package overview

import (
	"bufio"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/go-while/go-utils"
	"github.com/go-while/nntp-storage"
	//"encoding/gob"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	//"sync"
	"database/sql"
	"time"
	"unicode"
)

const (
	null string = "\x00"
	tab  string = "\010"

	XREF_PREFIX = "nntp" // todo: hardcoded fix: should be a variable hostname?

	MAX_FLUSH int64 = 5 // flush mmaps every N seconds

	LIMIT_SPLITMAX_NEWSGROUPS int = 25
	MAX_REF                   int = 30

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
	AUTOINDEX           bool   = true
	DEBUG_OV            bool   = false
	CR                  string = "\r"
	LF                  string = "\n"
	CRLF                string = CR + LF
	DOT                 string = "."
	DOTCRLF             string = DOT + CRLF
	PRELOAD_ZERO_1K     string
	PRELOAD_ZERO_4K     string
	PRELOAD_ZERO_128K   string
	PRELOAD_ZERO_1M     string
	open_mmap_overviews Open_MMAP_Overviews
	Known_msgids        Known_MessageIDs
	OV_handler          OV_Handler
	Overview            OV
	OVIndex             OverviewIndex
	open_request_chan   chan Overview_Open_Request
	close_request_chan  chan Overview_Close_Request
	//read_request_chan         chan Overview_Read_Request  // FIXME TODO
	workers_done_chan       chan int      // holds an empty struct for done overview worker
	count_open_overviews    chan struct{} // holds an empty struct for every open overview file
	MAX_Open_overviews_chan chan struct{} // locking queue prevents opening of more overview files
	OV_AUTOINDEX_CHAN       = make(chan *NEWOVI, 1)
	// The date format layouts to try
	NNTPDateLayoutsExtended = []string{

		"Monday, _2 Jan 2006 15:04:05 -0700",
		"Monday, _2 Jan 2006 15:04:05 -0700 (MST)",
		"Monday, _2 Jan 2006 15:04:05 MST",
		"Monday, _2 Jan 2006 15:04:05",
		"Monday, _2 Jan 2006 15:04",
		"Monday, _2 Jan 2006",
		"Monday, _2 January 2006 15:04:05 -0700",
		"Monday, _2 January 2006 15:04:05 -0700 (MST)",
		"Monday, _2 January 2006 15:04:05 MST",
		"Monday, _2 January 2006 15:04:05",
		"Monday, _2 January 2006 15:04",
		"Monday, _2 January 2006",

		"Monday, _2 Jan 06 15:04:05 -0700 (MST)",
		"Monday, _2 Jan 06 15:04:05 -0700",
		"Monday, _2 Jan 06 15:04:05 MST",
		"Monday, _2 Jan 06 15:04:05",
		"Monday, _2 Jan 06 15:04",
		"Monday, _2 Jan 06",
		"Monday, _2 January 06 15:04:05 -0700 (MST)",
		"Monday, _2 January 06 15:04:05 -0700",
		"Monday, _2 January 06 15:04:05 MST",
		"Monday, _2 January 06 15:04:05",
		"Monday, _2 January 06 15:04",
		"Monday, _2 January 06",

		"Mon, _2 Jan 2006 15:04:05 -0700",
		"Mon, _2 Jan 2006 15:04:05 -0700 (MST)",
		"Mon, _2 Jan 2006 15:04:05 MST",
		"Mon, _2 Jan 2006 15:04:05",
		"Mon, _2 Jan 2006 15:04 -0700",
		"Mon, _2 Jan 2006 15:04",
		"Mon, _2 Jan 2006",
		"Mon, _2 January 2006 15:04:05 -0700",
		"Mon, _2 January 2006 15:04:05 -0700 (MST)",
		"Mon, _2 January 2006 15:04:05 MST",
		"Mon, _2 January 2006 15:04:05",
		"Mon, _2 January 2006 15:04 -0700",
		"Mon, _2 January 2006 15:04",
		"Mon, _2 January 2006",
		// Tue, 22 Feb 94 06:15:56 Mst
		"Mon, _2 Jan 06 15:04:05 -0700 (MST)",
		"Mon, _2 Jan 06 15:04:05 -0700",
		"Mon, _2 Jan 06 15:04:05 MST",
		"Mon, _2 Jan 06 15:04:05",
		"Mon, _2 Jan 06 15:04",
		"Mon, _2 Jan 06",
		"Mon, _2 January 06 15:04:05 -0700 (MST)",
		"Mon, _2 January 06 15:04:05 -0700",
		"Mon, _2 January 06 15:04:05 MST",
		"Mon, _2 January 06 15:04:05",
		"Mon, _2 January 06 15:04",
		"Mon, _2 January 06",

		"_2 Jan 2006 15:04:05 -0700 (MST)",
		"_2 Jan 2006 15:04:05 -0700",
		"_2 Jan 2006 15:04:05 MST",
		"_2 Jan 2006 15:04:05",
		"_2 Jan 2006 15:04 MST",
		"_2 Jan 2006 15:04",
		"_2 Jan 2006",
		"_2 January 2006 15:04:05 -0700 (MST)",
		"_2 January 2006 15:04:05 -0700",
		"_2 January 2006 15:04:05 MST",
		"_2 January 2006 15:04:05",
		"_2 January 2006 15:04 MST",
		"_2 January 2006 15:04",
		"_2 January 2006",

		"_2 Jan 06 15:04:05 -0700 (MST)",
		"_2 Jan 06 15:04:05 -0700",
		"_2 Jan 06 15:04:05 MST",
		"_2 Jan 06 15:04:05",
		"_2 Jan 06 15:04",
		"_2 Jan 06",
		"_2 January 06 15:04:05 -0700 (MST)",
		"_2 January 06 15:04:05 -0700",
		"_2 January 06 15:04:05 MST",
		"_2 January 06 15:04:05",
		"_2 January 06 15:04",
		"_2 January 06",

		"Jan _2, 2006 15:04:05 -0700 (MST)",
		"Jan _2, 2006 15:04:05 -0700",
		"Jan _2, 2006 15:04:05 MST",
		"Jan _2, 2006 15:04:05",
		"Jan _2, 2006 15:04",
		"Jan _2, 2006",
		"January _2, 2006 15:04:05 -0700 (MST)",
		"January _2, 2006 15:04:05 -0700",
		"January _2, 2006 15:04:05 MST",
		"January _2, 2006 15:04:05",
		"January _2, 2006 15:04",
		"January _2, 2006",

		"Jan _2, 06 15:04:05 -0700 (MST)",
		"Jan _2, 06 15:04:05 -0700",
		"Jan _2, 06 15:04:05 MST",
		"Jan _2, 06 15:04:05",
		"Jan _2, 06 15:04",
		"Jan _2, 06",
		"January _2, 06 15:04:05 -0700 (MST)",
		"January _2, 06 15:04:05 -0700",
		"January _2, 06 15:04:05 MST",
		"January _2, 06 15:04:05",
		"January _2, 06 15:04",
		"January _2, 06",

		// weird formats from importing archives....
		// Friday, 20-Jun-10 16:14:55 GMT
		"Monday, _2-Jan-06 15:04:05 MST",
		// Sat, 26 Nov 1994 10:12:43 LOCAL
		"Mon, _2 Jan 2006 15:04:05 LOCAL",
		"Mon, _2 Jan 06 15:04:05 LOCAL",
		// Fri, 7 Oct 1994 17:12:13
		"Mon, _2 Jan 2006 15:04:05 UNDEFINED",
		"Mon, _2 Jan 06 15:04:05 UNDEFINED",
		// August 20, 2009 10:49:52 AM CEST
		"January _2, 2006 15:04:05 PM MST",
		"Jan _2, 2006 15:04:05 PM MST",
		// Wed, 31-Dec-69 18:59:59 EDT
		"Mon, _2-Jan-06 15:04:05 MST",
		// Fri Apr 8 00:49:21 1983
		"Mon Jan _2 15:04:05 2006",
		// Thu, 28 Feb 2002 16: 8:48 GMT
		"Mon, _2 Jan 2006 15: 4:05 MST",
		// Tue, 27 Dec 1994 23:8:0 GMT
		"Mon, _2 Jan 2006 15:4:5 MST",
		"Mon, _2 Jan 2006 15:4:5",
		"Mon, _2 Jan 2006 15:4:05 MST",
		"Mon, _2 Jan 2006 15:4:05",
		// Tue, 6 Sep 94 21:2:22 GMT
		"Mon, _2 Jan 06 15:4:5 MST",
		"Mon, _2 Jan 06 15:4:5",
		"Mon, _2 Jan 06 15:4:05 MST",
		"Mon, _2 Jan 06 15:4:05",
		// Fri,23 Feb 2001 17:14:11 MST'
		"Mon,_2 Jan 2006 15:04:05 MST",
		// Fri,23 Feb 2001 17:14:11+2000'
		"Mon,_2 Jan 2006 15:04:05-0700",
		// Tue,12 May 2005 13:33:40 GMT +0200
		"Mon,_2 Jan 2006 15:04:05 MST -0700",
		// Mon, 9 July 2001 12:45:45 -0700
		"Mon, _2 January 2006 15:04:05 -0700",
		// Monday, 6 June 1994 17:07:49 PDT
		"Monday, _2 January 2006 15:04:05 MST",
		// Wed,12 Jun 2002 09:00:46
		"Mon,_2 Jan 2006 15:04:05",
		// 20 oct. 2009 22:05:25
		"_2 Jan. 2006 15:04:05",
		// 16 Jul 2002 19:8:33
		"_2 Jan 2006 15:4:5",
		// 16 April 1994 13:01:21 EDT
		"_2 January 2006 15:04:05 MST",
		// 22 Apr 92 14:2:44 GMT
		"_2 Jan 06 15:4:5 MST",
		"_2 Jan 06 15:4:05 MST",
		// 29Aug 2006 21:11:48 -0600
		"_2Jan 2006 15:04:05 -0700",
		// Wes, 23 Jun 2010 11:24:30 -0500
		"_2 Jan 2006 15:04:05 -0700",
		// 21-Jan-2005 13:38:02 PST
		"_2-Jan-2006 15:04:05 MST -0700",
		"_2-Jan-2006 15:04:05 MST",
		"_2-Jan-2006 15:04:05 -0700",
		// Mon, 17 Nov 2003 10 53 35 +0100
		"Mon, _2 Jan 2006 15 04 05 MST -0700",
		"Mon, _2 Jan 2006 15 04 05 -0700",
		// , 10 Dec 2006 16:55:9 -0000
		", _2 Jan 2006 15:04:5 -0700",
		", _2 Jan 2006 15:4:5 -0700",
		// Sat 23 Jun 2023 23:23:23 PM UTC
		"Mon _2 Jan 2006 15:04:05 PM MST",
		// Monday,02 July 2001 12:23:48 AM
		"Monday,_2 Jan 2006 15:04:05 PM MST",
		// Sun,31 Dec 95 17:11:30 -0500
		"Mon,_2 Jan 06 15:04:05 -0700",
		// Fr, 20 Feb 2004 16:11:20 +0100
		"Mo, _2 Jan 2006 15:04:05 -0700",
		// Fri,28 Sep 94 16:11:41 GMT
		"Mon,_2 Jan 06 15:04:05 MST",
		// Wen, 17 May 2006 10:11:41 -0100
		"Mon, _2 Jan 2006 15:04:05 -0700",
		// Fri, 23-Apr-2004 23:43:27 +0200
		"Mon, _2-Jan-2006 15:04:05 -0700",
		// Fri 31 Aug 2012 22:45:37 -0400
		"Mon _2 Jan 2006 15:04:05 -0700",
		// Sun, Jun 17 2001 23:17:17 GMT+0200
		"Mon, Jan _2 2006 15:04:05 MST-0700",
		// 18-Feb-90 20:52 CST
		"_2-Jan-06 15:04 MST",
		// Date: Tue, 19 Jun 2007 13:09:02 GMT
		"Date: Tue, _2 Jun 2006 15:04:05 MST",
	}
	NNTPDateLayouts = NNTPDateLayoutsExtended
	/*
	NNTPDateLayouts = []string{

	}
	*/
)

type Overview_Open_Request struct {
	hash       string              // hash from group (hash.overview file)
	file_path  string              // grouphash.overview file path
	reply_chan chan Overview_Reply // replies back with the 'OVFH' or err
}

type Overview_Close_Request struct {
	force_close bool                // will be set to true if this overview has to be closed
	ovfh        *OVFH               // overview mmap file handle struct
	reply_chan  chan Overview_Reply // replies back with the 'OVFH' or err
}

type Overview_Reply struct {
	ovfh    *OVFH // overview mmap file handle struct
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
	Retchan        chan []*ReturnChannelData
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

	if AUTOINDEX {
		go OV_AutoIndex()
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
			log.Printf("Load_Overview: cache known_messageids=%d", known_messageids)
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

		if debug_OV_handler {
			log.Printf("Load_Overview maxworkers=%d, max_queue_size=%d, max_open_mmaps=%d, known_messageids=%d, ov_opener=%d, ov_closer=%d, stop_server_chan='%v' debug_OV_handler=%t len(MAX_Open_overviews_chan)=%d",
				maxworkers, max_queue_size, max_open_mmaps, known_messageids, ov_opener, ov_closer, stop_server_chan, debug_OV_handler, len(MAX_Open_overviews_chan))
		}
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

		workers_done := len(workers_done_chan)                          // int
		open_overviews := len(count_open_overviews)                     // int
		queued_overviews := len(Overview.OVIC)                          // int
		rc_head_chan := len(storage.WriteCache.WC_head_chan)            // int
		wc_head_chan := len(storage.ReadCache.RC_head_chan)             // int
		wc_body_chan := len(storage.WriteCache.WC_body_chan)            // int
		rc_body_chan := len(storage.ReadCache.RC_body_chan)             // int
		cache_history := len(storage.WriteCache.Log_cache_history_chan) // int
		xrefs := len(storage.XrefLinker.Xref_link_chan)                 // int
		all_ov_workers_done := workers_done == maxworkers               // bool
		logstr = fmt.Sprintf("workers_done=%d/%d open_overviews=%d queued_overviews=%d wc_head_chan=%d wc_body_chan=%d rc_head_chan=%d rc_body_chan=%d cache_history=%d xrefs=%d", workers_done, maxworkers, open_overviews, queued_overviews, wc_head_chan, wc_body_chan, rc_head_chan, rc_body_chan, cache_history, xrefs)

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
	who := fmt.Sprintf("OVW:%d", ov_wid)
	did, max := 0, 10000
	//log.Printf("%s Alive", who)

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
				log.Printf("%s overview_Worker %d) got ovl msgid='%s'", who, ov_wid, ovl.Messageid)
			}
			// handle incoming overview line
			ovl.Retchan <- ov.di_ov(who, ovl) // passes overview from divide_incoming_overview directly into the Retchan
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

func (ov *OV) di_ov(who string, ovl OVL) []*ReturnChannelData {
	// divide_incoming_overview
	dones := 0
	retlist := []*ReturnChannelData{}
	var retchans []chan ReturnChannelData
	overviewline := Construct_OVL(ovl)

	for _, newsgroup := range ovl.Newsgroups {

		hash := strings.ToLower(utils.Hash256(newsgroup))

		if ov.more_parallel { // constantly spawning of new go routines eats memory

			retchan := make(chan ReturnChannelData, 1)
			retchans = append(retchans, retchan)
			go ov.GO_pi_ov(who, overviewline, newsgroup, hash, ovl.ReaderCachedir, retchan)

		} else {

			if retdata := ov.GO_pi_ov(who, overviewline, newsgroup, hash, ovl.ReaderCachedir, nil); retdata.Retbool == true {
				retlist = append(retlist, &retdata)
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
					retlist = append(retlist, &retdata)
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
			log.Printf("%s di_ov dones=%d len_ovl.Newsgroups=%d retchans=%d retlist=%d", who, dones, len(ovl.Newsgroups), len(retchans), len(retlist))
		}
	} else {
		log.Printf("%s ERROR di_ov dones=%d len_ovl.Newsgroups=%d retchans=%d retlist=%d", who, dones, len(ovl.Newsgroups), len(retchans), len(retlist))
	}
	return retlist
} // end func divide_incoming_overview

func Construct_OVL(ovl OVL) string {
	// construct an overview line
	MAX_REF := 100
	ref_len, ref_len_limit := 0, 16384
	var references string
	for i, ref := range ovl.References {
		len_ref := len(ref)
		new_ref_len := ref_len + len_ref
		if new_ref_len > ref_len_limit {
			log.Printf("BREAK Construct_OVL new_ref_len=%d msgid='%s' i=%d", new_ref_len, ovl.Messageid, i)
			break
		}
		if i >= MAX_REF {
			log.Printf("BREAK Construct_OVL new_ref_len=%d msgid='%s' i=%d/%d", new_ref_len, ovl.Messageid, i, len(ovl.References))
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

func (ov *OV) GO_pi_ov(who string, overviewline string, newsgroup string, hash string, cachedir string, retchan chan ReturnChannelData) ReturnChannelData {
	// GO_process_incoming_overview
	// GO_pi_ov can run concurrently!

	<-MAX_Open_overviews_chan    // get a global lock to limit total num of open overview files
	defer return_overview_lock() // return the lock whenever func returns

	var err error
	mmap_file_path := cachedir + "/" + hash + ".overview"

	ovfh, err := Open_ov(who, mmap_file_path)
	if err != nil || ovfh == nil {
		log.Printf("%s ERROR overview.Open_ov ovfh='%v 'err='%v'", who, ovfh, err)
		return fail_retchan(retchan)

	}
	if ovfh.Mmap_handle == nil {
		log.Printf("%s ERROR GO_pi_ov ovfh.Mmap_handle=nil", who)
		return fail_retchan(retchan)
	}
	if DEBUG_OV {
		log.Printf("%s overview.Open_ov ng='%s' OK", who, newsgroup)
	}

	if ovfh.Last == 0 {
		ovfh.Last = 1
	}

	xref := fmt.Sprintf(XREF_PREFIX+" %s:%d", newsgroup, ovfh.Last)
	ovl_line := fmt.Sprintf("%d\t%s\t%s\n", ovfh.Last, overviewline, xref)
	new_ovfh, err, errstr := Write_ov(who, ovfh, ovl_line, false, false, false, false)
	if err != nil {
		log.Printf("%s ERROR GO_pi_ovWrite_ovfh err='%v' errstr='%s'", who, err, errstr)
		return fail_retchan(retchan)

	} else {
		if new_ovfh != nil && new_ovfh.Mmap_handle != nil {
			ovfh = new_ovfh
		}
		if DEBUG_OV {
			log.Printf("%s GO_pi_ov OK ng='%s' ovfh.Findex=%d ovfh.Last=%d", who, newsgroup, ovfh.Findex, ovfh.Last)
		}

		if _, err := Update_Footer(who, ovfh, "GO_pi_ov"); err != nil {
			log.Printf("%s ERROR overview.Update_Footer ng='%s' err='%v'", who, newsgroup, err)
			return fail_retchan(retchan)

		} else {
			if DEBUG_OV {
				log.Printf("%s overview.Update_Footer OK ng='%s' ovfh.Findex=%d ovfh.Last=%d", who, newsgroup, ovfh.Findex, ovfh.Last)
			}
		}
	}
	ovfh.Last++

	// finally close the mmap
	if DEBUG_OV {
		log.Printf("%s p_i_o: Closing fp='%s'", who, ovfh.File_path)
	}
	if err := Close_ov(who, ovfh, true, false); err != nil {
		log.Printf("%s p_i_o: ERROR FINAL Close_ovfh err='%v'", who, err)
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
		log.Printf("ERROR Split_NewsGroups msgid='%s' inputX='%x' returned=%d o=%d e=%d len_dirty=%d > local.trash", msgid, newsgroups_str, len(newsgroups), o, e, len_dirty)
	}
	return newsgroups
} // end func Split_NewsGroups

func IsValidGroupName(group string) bool {
	if group == "" {
		log.Printf("!IsValidGroupName group='%s' empty", group)
		return false
	}

	// first char of group has to be a letter or digit and not uppercase
	for _, r := range group[:1] {

		if unicode.IsUpper(r) {
			//log.Printf("!IsValidGroupName IsUpper i=%d group='%s'", i, group)
			return false
		}

		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			//log.Printf("!IsValidGroupName !IsLetter !IsDigit i=%d group='%s'", i, group)
			return false
		}
	} // end for

	if len(group) > 127 {
		log.Printf("!IsValidGroupName group='%s' len=%d", group, len(group))
		return false
	}

	if len(group) > 1 {

		if group[len(group)-1] == '.' { // last cant be dot
			return false
		}

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
			log.Printf("!IsValidGroupName #3 groupX='%x' i=%d r=x'%x'", group, i, string(r))
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

func Test_Overview(who string, file_path string, DEBUG bool) bool {
	// test should never try to update the footer!
	update_footer := false
	/*hash, err := get_hash_from_filename(file_path)
	if err != nil {
		log.Printf("%s ERROR Test_Overview get_hash_from_filename='%s' err='%v'", file_path, err)
		return false
	}*/
	if ovfh, err := Open_ov(who, file_path); err != nil {
		//if ovfh, err := handle_open_ov(who, hash, file_path); err != nil {
		log.Printf("%s ERROR OV TEST_Overview Open_ov err='%v' fp='%s'", who, err, filepath.Base(file_path))
		return false

	} else {
		if err := Close_ov(who, ovfh, update_footer, true); err != nil {
			log.Printf("%s ERROR OV TEST_Overview Close_ov err='%v' fp='%s'", who, err, filepath.Base(file_path))
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
		return "", fmt.Errorf("ERROR get_hash_from_file file_path='%s' !HasSuffix .overview", filepath.Base(file_path))
	}

	hash := strings.Split(file, ".")[0]
	if len(hash) < 32 {
		return "", fmt.Errorf("ERROR Open_ov len(hash) < 32 fp='%s'", filepath.Base(file_path))
	}
	return hash, nil
}

func Open_ov(who string, file_path string) (*OVFH, error) {
	var err error
	var hash string
	if hash, err = get_hash_from_filename(file_path); err != nil {
		err = fmt.Errorf("%s ERROR Open_ov -> hash err='%v' fp='%s'", who, err, filepath.Base(file_path))
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
	if DEBUG_OV {
		log.Printf("%s SENDING Open_ov open_request to open_request_chan fp='%s'", who, filepath.Base(file_path))
	}
	open_request_chan <- open_request

	// wait for reply
	if DEBUG_OV {
		log.Printf("%s WAITING Open_ov -> reply_chan fp='%s'", who, filepath.Base(file_path))
	}
	reply := <-reply_chan
	// got reply
	if DEBUG_OV {
		log.Printf("%s GOT REPLY Open_ov -> reply_chan fp='%s'", who, filepath.Base(file_path))
	}

	if reply.err == nil {

		if DEBUG_OV {
			log.Printf("%s OK REPLY Open_ov -> fp='%s'", who, filepath.Base(file_path))
		}
		return reply.ovfh, nil

	} else {
		err = reply.err
		log.Printf("%s ERROR REPLY Open_ov -> reply_chan err='%v' fp='%s'", who, err, filepath.Base(file_path))
	}

	return nil, fmt.Errorf("%s ERROR Open_ov final err='%v' fp='%s'", who, err, filepath.Base(file_path))
} // end func Open_ov

func handle_open_ov(who string, hash string, file_path string) (*OVFH, error) {
	var err error
	var file_handle *os.File
	var mmap_handle mmap.MMap
	var ov_footer string
	cs := 0

	/*
		file_path_new := file_path + ".new"
		if utils.FileExists(file_path_new) {
			log.Printf("%s INFO handle_open_ov fp='%s.new'", who, file_path)
			file_path = file_path_new
		} else
	*/
	if !utils.FileExists(file_path) {
		return nil, fmt.Errorf("%s ERROR handle_open_ov !fileExists fp='%s'", who, file_path)
	}

	if file_handle, err = os.OpenFile(file_path, os.O_RDWR, 0644); err != nil {
		log.Printf("%s ERROR handle_open_ov -> mmap.Map err='%v' cs=%d fp='%s'", who, err, cs, file_path)
		return nil, err
	}
	cs++ // 1

	if mmap_handle, err = mmap.Map(file_handle, mmap.RDWR, 0); err != nil || mmap_handle == nil {
		log.Printf("%s ERROR handle_open_ov -> Read_Head_ov err='%v' mmap_handle='%v' cs=%d fp='%s'", who, err, mmap_handle, cs, file_path)
		return nil, err
	}
	cs++ // 2

	time_open, time_flush, written := utils.Now(), utils.Now(), 0

	if DEBUG_OV {
		log.Printf("%s handle_open_ov new OVFH fp='%s'", who, file_path)
	}
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

	if _, err := Read_Head_ov(who, ovfh); err != nil { // _ = ov_header
		log.Printf("%s ERROR handle_open_ov -> Read_Head_ov err='%v' cs=%d fp='%s'", who, err, cs, file_path)
		return nil, err
	}
	cs++ // 3

	if ov_footer, err = Read_Foot_ov(who, ovfh); err != nil {
		log.Printf("%s ERROR handle_open_ov -> Read_Foot_ov err='%v' cs=%d fp='%s'", who, err, cs, file_path)
		return nil, err
	}
	cs++ // 4

	foot := strings.Split(ov_footer, ",")
	if len(foot) != SIZEOF_FOOT {
		log.Printf("%s ERROR Open_ov -> len(foot)=%d != SIZEOF_FOOT=%d fp='%s'", len(foot), SIZEOF_FOOT, file_path)
		return nil, fmt.Errorf("ERROR handle_open_ov cs=%d fp='%s'", who, cs, file_path)
	}
	cs++ // 5

	if !strings.HasPrefix(foot[1], "last=") ||
		!strings.HasPrefix(foot[2], "Findex=") ||
		!strings.HasPrefix(foot[3], "bodyend=") ||
		!strings.HasPrefix(foot[4], "fend=") {
		log.Printf("%s ERROR Open_ov -> error !HasPrefix foot fp='%s'", who, file_path)
		return nil, fmt.Errorf("%s ERROR handle_open_ov cs=%d fp='%s'", who, cs, file_path)
	}
	cs++ // 6

	last, findex := utils.Str2uint64(strings.Split(foot[1], "=")[1]), utils.Str2int(strings.Split(foot[2], "=")[1])
	bodyend, fend := utils.Str2int(strings.Split(foot[3], "=")[1]), utils.Str2int(strings.Split(foot[4], "=")[1])
	if findex >= bodyend && bodyend != fend-OV_RESERVE_END {
		log.Printf("%s ERROR Open_ov -> findex=%d > bodyend=%d ? fend=%d fp='%s'", who, findex, bodyend, fend, file_path)
		return nil, fmt.Errorf("%s ERROR handle_open_ov cs=%d fp='%s'", who, cs, file_path)
	}
	cs++ // 7

	if findex < OV_RESERVE_BEG || findex > OV_RESERVE_BEG && last == 0 {
		log.Printf("%s ERROR Open_ov -> findex=%d OV_RESERVE_BEG=%d last=%d fp='%s'", who, findex, OV_RESERVE_BEG, last, file_path)
		return nil, fmt.Errorf("%s ERROR handle_open_ov cs=%d fp='%s'", who, cs, file_path)
	}
	cs++ // 8

	ovfh.Findex = findex
	ovfh.Last = last
	if Replay_Footer(who, ovfh) {
		if DEBUG_OV {
			log.Printf("%s handle_open_ov -> Replay_Footer OK fp='%s'", who, file_path)
		}
		return ovfh, nil
	} else {
		log.Printf("%s ERROR handle_open_ov -> !Replay_Footer fp='%s'", who, file_path)

	}
	log.Printf("%s handle_open_ov !OK fp='%s'", who, file_path)
	return nil, fmt.Errorf("%s ERROR handle_open_ov cs=%d fp='%s' final", who, cs, file_path)
} // end func handle_open_ov

func Close_ov(who string, ovfh *OVFH, update_footer bool, force_close bool) error {
	var err error
	if ovfh == nil {
		err = fmt.Errorf("%s Error Close_ov ovfh=nil update_footer=%t force_close=%t", who, update_footer, force_close)
		return err
	}
	file := filepath.Base(ovfh.File_path)
	var close_request Overview_Close_Request
	reply_chan := make(chan Overview_Reply, 1) // FIXME: mark*836b04be* can we not create the reply_chan everytime? just pass it from upper end to here and reuse it?
	close_request.ovfh = ovfh
	close_request.reply_chan = reply_chan
	if force_close {
		close_request.force_close = true
	}
	close_request_chan <- close_request
	// wait for reply
	if DEBUG_OV {
		log.Printf("%s WAITING Close_ov -> reply_chan fp='%s'", who, file)
	}
	reply := <-reply_chan
	// got reply
	if DEBUG_OV {
		log.Printf("%s GOT REPLY Close_ov -> reply_chan fp='%s'", who, file)
	}
	if reply.err == nil {
		if DEBUG_OV {
			log.Printf("%s OK REPLY Close_ov -> fp='%s'", who, file)
		}
	} else {
		err = reply.err
		log.Printf("%s ERROR REPLY Close_ov -> reply_chan err='%v' fp='%s'", who, err, file)
	}

	return err
} // end func Close_ov

func handle_close_ov(who string, ovfh *OVFH, update_footer bool, force_close bool, grow bool) error {
	var err error
	if update_footer {
		if DEBUG_OV {
			log.Printf("%s handle_close_ov update_footer=%t grow=%t fp='%s'", who, update_footer, grow, ovfh.File_path)
		}
		if _, err := Update_Footer(who, ovfh, "handle_close_ov"); err != nil {
			return err
		}
	}

	if err = ovfh.Mmap_handle.Flush(); err != nil {
		return err
	}
	if err = ovfh.Mmap_handle.Unmap(); err != nil {
		log.Fatalf("Error ovfh.Mmap_handle.Unmap err='%v'", err)
		return err
	}
	if err = ovfh.File_handle.Close(); err != nil {
		return err
	}
	if DEBUG_OV {
		log.Printf("%s handle_close_ov update_footer=%t grow=%t OK fp='%s'", who, update_footer, grow, ovfh.File_path)
	}
	if err != nil {
		log.Printf("%s ERROR handle_close_ov fp='%s' err='%v'", who, ovfh.File_path, err)
	}
	return err
} // end func handle_close_ov

func Flush_ov(who string, ovfh *OVFH) error {
	var err error
	if err = ovfh.Mmap_handle.Flush(); err != nil {
		log.Printf("%s ERROR Flush_ov fp='%s' err='%v'", who, ovfh.File_path, err)
	} else {
		if DEBUG_OV {
			log.Printf("%s Flush_ov OK fp='%s'", who, ovfh.File_path)
		}
	}
	return err
} // end func Flush_ov

func Replay_Footer(who string, ovfh *OVFH) bool {

	if ovfh.Last == 0 && ovfh.Findex == OV_RESERVE_BEG {
		if DEBUG_OV {
			log.Printf("%s Replay_Footer NEW OK ovfh.Findex=%d", who, ovfh.Findex)
		}
		return true
	}
	position, frees, newlines := 1, 0, 0
	needs_trues := 2
	startindex, endindex := ovfh.Findex, 0
	tabs, trues := 0, 0

replay:
	for {
		c := ovfh.Mmap_handle[startindex] // check char at index
		if DEBUG_OV {
			//log.Printf("%s DEBUG REPLAY: c='%x' startindex=%d", who, c, startindex)
		}

		switch c {
		case 0:
			// char is a <nul> and has to be at 1st position
			if position == 1 {
				//trues++
			} else {
				log.Printf("%s ERROR OV c=nul position=%d startindex=%d endindex=%d Findex=%d fp='%s'", who, position, startindex, endindex, ovfh.Findex, ovfh.File_path)
			}
			frees++ // should only be 1 free byte, always
			if frees > 1 {
				log.Printf("%s ERROR OV frees=%d > 1 position=%d startindex=%d endindex=%d Findex=%d fp='%s'", who, frees, position, startindex, endindex, ovfh.Findex, ovfh.File_path)
			}
		case '\t':
			// char is a <tab>
			tabs++
			if tabs > OVERVIEW_TABS {
				log.Printf("%s ERROR OV tabs=%d > %d position=%d startindex=%d fp='%s'", who, tabs, OVERVIEW_TABS, position, startindex, ovfh.File_path)
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
					log.Printf("%s ERROR OV 1st newline position=%d != 2 space=%d fp='%s'", who, position, space, ovfh.File_path)
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
			log.Printf("%s ERROR Replay Footer found 2 newlines but failed to confirm content?! fp='%s'", who, ovfh.File_path)
			// anyways found 2 newlines, break and see checks
			break
		}

		if startindex < OV_RESERVE_BEG {
			// dont run into the header...
			log.Printf("%s ERROR Replay Footer startindex=%d < OV_RESERVE_BEG=%d fp='%s'", who, startindex, OV_RESERVE_BEG, ovfh.File_path)
			break
		}
		startindex-- // decrease index to walk backwards
		position++   // count position upwards

	} // end for reverse search

	retstring := fmt.Sprintf("needs_trues=%d trues=%d frees=%d newlines=%d tabs=%d position=%d startindex=%d endindex=%d fp='%s'",
		needs_trues, trues, frees, newlines, tabs, position, startindex, endindex, ovfh.File_path)

	if trues != needs_trues {
		log.Printf("%s ERROR Replay Footer %s", who, retstring)
		return false
	}

	if tabs != OVERVIEW_TABS {
		log.Printf("%s ERROR Replay_Footer lastline startindex=%d tabs=%d != OVERVIEW_TABS=%d", who, startindex, tabs, OVERVIEW_TABS)
		return false
	}

	if endindex == 0 || endindex < startindex {
		log.Printf("%s ERROR Replay_Footer endindex=%d < startindex=%d", who, endindex, startindex)
		return false
	}

	if DEBUG_OV {
		log.Printf("%s OK Replay Footer %s", who, retstring)
	}

	//log.Printf("Replay_Footer PRE tabs=%d newlines=%d frees=%d", tabs, newlines, frees)

	// read the last line and get the first field with last written article number to file,
	// which should be the ovh.Last value - 1
	lastline := string(ovfh.Mmap_handle[startindex+1 : endindex])
	if DEBUG_OV {
		log.Printf("%s Replay_Footer lastline='%s'", who, lastline)
	}
	last_str := strings.Split(lastline, "\t")[0]
	if frees > 1 {
		oldFindex := ovfh.Findex
		diff := frees - 1
		ovfh.Findex -= diff
		log.Printf("%s WARN OV adjusted frees=%d oldFindex=%d - diff=%d = Findex=%d", who, frees, oldFindex, diff, ovfh.Findex)
		time.Sleep(5 * time.Second) // DEBUG SLEEP

	}
	if utils.IsDigit(last_str) {
		if utils.Str2uint64(last_str) == ovfh.Last-1 {
			if DEBUG_OV {
				log.Printf("%s Replay_Footer OK fp='%s' last=%s next=%d", who, ovfh.File_path, last_str, ovfh.Last)
			}
			return true
		}
	}
	log.Printf("%s ERROR Replay_Footer last=%s ovfh.Last=%d  fp='%s'", who, last_str, ovfh.Last, filepath.Base(ovfh.File_path))
	return false
} // end Replay_Footer

func Write_ov(who string, ovfh *OVFH, data string, is_head bool, is_foot bool, grow bool, delete bool) (*OVFH, error, string) {
	var err error
	//len_data := len(data)
	databyte := []byte(data)
	len_data := len(databyte)
	mmap_size := len(ovfh.Mmap_handle)
	if DEBUG_OV {
		log.Printf("%s Write_ov len_data=%d is_head=%t is_foot=%t fp='%s'", who, len_data, is_head, is_foot, filepath.Base(ovfh.File_path))
	}

	// set start Findex vs reserved space at beginning of map
	if !is_head && !is_foot && ovfh.Findex < OV_RESERVE_BEG {
		ovfh.Findex = OV_RESERVE_BEG
	}

	if mmap_size != ovfh.Mmap_size { // unsure if this could change while mapped we have serious trouble
		return nil, fmt.Errorf("%s ERROR Write_ov len(ovfh.Mmap_handle)=%d != ovfh.Mmap_size=%d fp='%s'", who, mmap_size, ovfh.Mmap_size, filepath.Base(ovfh.File_path)), ""
	}

	if ovfh.Mmap_range < OV_RESERVE_BEG+OV_RESERVE_END {
		return nil, fmt.Errorf("%s ERROR Write_ov ovfh.Mmap_size=%d fp='%s'", who, ovfh.Mmap_size, filepath.Base(ovfh.File_path)), ""
	}

	// total bodyspace of overview file
	bodyspace := ovfh.Mmap_size - OV_RESERVE_BEG - OV_RESERVE_END
	if bodyspace <= 0 {
		return nil, fmt.Errorf("%s ERROR Write_ov bodyspace=%d fp='%s'", who, bodyspace, filepath.Base(ovfh.File_path)), ""
	}

	// dont count OV_RESERVE_BEG here as it is already included in Findex
	bodyend := ovfh.Mmap_range - OV_RESERVE_END
	freespace := bodyend - ovfh.Findex
	// newbodysize adds len of data to Findex
	// then newbodysize should not be higher than freespace
	newbodysize := len_data + ovfh.Findex

	if DEBUG_OV {
		log.Printf("%s Write_ov bodyspace=%d len_data=%d freespace=%d Findex=%d newsize=%d", who, len_data, bodyspace, freespace, ovfh.Findex, newbodysize)
	}

	var new_ovfh *OVFH
	if !is_foot && (freespace <= 1 || newbodysize >= bodyend) {
		if DEBUG_OV {
			log.Printf("%s Write_ov GROW OVERVIEW Findex=%d len_data=%d freespace=%d bodyend=%d newsize=%d hash='%s'", who, ovfh.Findex, len_data, freespace, bodyend, newbodysize, ovfh.Hash)
		}
		pages, blocksize := 1, "128K"
		if newbodysize < 1024*1024 {
			pages, blocksize = 1, "4K" // grow by 4K
		} else if newbodysize >= 1024*1024 && newbodysize < 8*1024*1024 {
			pages, blocksize = 4, "4K" // grow by 16K
		} else if newbodysize >= 8*1024*1024 && newbodysize < 32*1024*1024 {
			pages, blocksize = 8, "4K" // grow by 64K
		} else if newbodysize >= 32*1024*1024 {
			pages, blocksize = 1, "128K" // grow by 128K
		}
		new_ovfh, err = Grow_ov(who, ovfh, pages, blocksize, 0, delete)
		if err != nil || new_ovfh == nil || new_ovfh.Mmap_handle == nil || len(new_ovfh.Mmap_handle) == 0 {
			overflow_err := fmt.Errorf("%s ERROR Write_ovfh -> Grow_ov err='%v' newsize=%d avail=%d mmap_size=%d fp='%s' mmaphandle=%d", who, err, newbodysize, freespace, new_ovfh.Mmap_size, filepath.Base(new_ovfh.File_path), len(new_ovfh.Mmap_handle))
			return nil, overflow_err, ERR_OV_OVERFLOW
		}
		if DEBUG_OV {
			log.Printf("%s Write_ov DONE GROW OVERVIEW hash='%s'", who, ovfh.Hash)
		}
		if new_ovfh != nil && new_ovfh.Mmap_handle != nil {
			ovfh = new_ovfh
		}
	}

	if is_foot && data == "" && grow == true && !delete {
		if ovfh.Mmap_handle == nil || (ovfh.Mmap_size != len(ovfh.Mmap_handle) || (ovfh.Mmap_range != len(ovfh.Mmap_handle)-1)) {
			err = fmt.Errorf("%s ERROR Write_ov is_foot data='' grow=true Findex=%d ovfh.Mmap_size=%d Mmap_handle=%d fp='%s'", who, ovfh.Findex, ovfh.Mmap_size, len(ovfh.Mmap_handle), filepath.Base(ovfh.File_path))
			return nil, err, ""
		}
		// zerofill-overwrite the footer space
		index := ovfh.Mmap_size - OV_RESERVE_END
		databyte := []byte(zerofill(ZERO_PATTERN, OV_RESERVE_END))
		// writes data to mmap byte for byte
		for pos, abyte := range databyte {
			if DEBUG_OV {
				log.Printf("write databyte index=%d/%d %d/%d bytes=%d delete=%t", index, ovfh.Mmap_range, pos, len(databyte)-1, len(databyte), delete)
			}
			if index >= ovfh.Mmap_size {
				log.Printf("%s Write_ov GROW fp='%s' reached end index=%d mmap_size=%d pos=%d len_databyte=%d break", who, filepath.Base(ovfh.File_path), index, ovfh.Mmap_size, pos+1, len(databyte))
				break
			}
			ovfh.Mmap_handle[index] = abyte
			index++
		}
	} else if is_foot && data == "" && grow == true && delete {
		if ovfh.Mmap_handle == nil || (ovfh.Mmap_size != len(ovfh.Mmap_handle) || (ovfh.Mmap_range != len(ovfh.Mmap_handle)-1)) {
			err = fmt.Errorf("%s ERROR Write_ov is_foot data='' grow=true Findex=%d ovfh.Mmap_size=%d Mmap_handle=%d fp='%s'", who, ovfh.Findex, ovfh.Mmap_size, len(ovfh.Mmap_handle), filepath.Base(ovfh.File_path))
			return nil, err, ""
		}
		// zerofill-overwrite the footer space
		index := ovfh.Findex
		databyte := []byte(zerofill(ZERO_PATTERN, ovfh.Mmap_size-index))
		// writes data to mmap byte for byte
		for pos, abyte := range databyte {
			if index >= ovfh.Mmap_size {
				log.Printf("%s Write_ov GROW fp='%s' reached end index=%d mmap_size=%d pos=%d len_databyte=%d break", who, filepath.Base(ovfh.File_path), index, ovfh.Mmap_size, pos+1, len(databyte))
				break
			}
			if DEBUG_OV {
				log.Printf("write databyte index=%d/%d %d/%d bytes=%d delete=%t", index, ovfh.Mmap_range, pos, len(databyte)-1, len(databyte), delete)
			}
			ovfh.Mmap_handle[index] = abyte
			index++
		}
		log.Printf(" --> wrote databyte ovfh.Findex=%d index=%d/%d bytes=%d delete=%t", ovfh.Findex, index, ovfh.Mmap_range, len(databyte), delete)

	} else if is_foot && data != "" { // footer data is not empty, write footer
		startindex := ovfh.Mmap_size - OV_RESERVE_END
		// writes data to mmap byte for byte
		for _, abyte := range databyte {
			ovfh.Mmap_handle[startindex] = abyte
			startindex++
		}
		ovfh.Written += len_data

	} else if !is_head && !is_foot && data != "" { // write data
		if ovfh.Mmap_handle == nil {
			err = fmt.Errorf("%s ERROR Write_ov #345 data=%d handle=%d fp='%s'", who, len(data), len(ovfh.Mmap_handle), filepath.Base(ovfh.File_path))
			return nil, err, ""
		}
		startindex := ovfh.Findex
		limit := ovfh.Mmap_range - OV_RESERVE_END
		if ovfh.Mmap_handle == nil {
			return nil, fmt.Errorf("%s ERROR Write_ov Mmap_handle == nil fp='%s'", who, filepath.Base(ovfh.File_path)), ""
		}
		if DEBUG_OV {
			log.Printf("%s Write_ov #346 data=%d Findex=%d limit=%d range=%d handle=%d fp='%s' delete=%t", who, len(data), startindex, limit, ovfh.Mmap_range, len(ovfh.Mmap_handle), filepath.Base(ovfh.File_path), delete)
		}

		// writes data to mmap byte for byte
		for _, abyte := range databyte {
			if startindex >= limit {
				break
			}
			ovfh.Mmap_handle[startindex] = abyte
			startindex++
		}
		ovfh.Written += len_data
		if !delete {
			ovfh.Findex = startindex
		}

	} // !is_head && ! is_foot

	if new_ovfh != nil && new_ovfh.Mmap_handle != nil {
		if DEBUG_OV {
			log.Printf("Write_ov returned new_ovfh err='%v' errstr=''", err)
		}
		return new_ovfh, err, ""
	}
	if err != nil {
		log.Printf("Write_ov returned ovfh=nil err='%v' errstr=''", err)
	}
	return nil, err, ""
} // end func Write_ov

func Read_Head_ov(who string, ovfh *OVFH) (string, error) {
	if ovfh.Mmap_size > OV_RESERVE_BEG {
		ov_header := string(ovfh.Mmap_handle[0:OV_RESERVE_BEG])
		if check_ovfh_header(who, ov_header) {
			return ov_header, nil
		} else {
			return "", fmt.Errorf("%s ERROR Read_Head_ov -> check_ovfh_header'", who)
		}
	}
	return "", fmt.Errorf("%s ERROR Read_Head_ov 'mmap_size=%d < OV_RESERVE_BEG=%d'", who, ovfh.Mmap_size, OV_RESERVE_BEG)
} // end func Read_Head_ov

func Read_Foot_ov(who string, ovfh *OVFH) (string, error) {
	foot_start := ovfh.Mmap_size - OV_RESERVE_END
	if foot_start > OV_RESERVE_END {
		ov_footer := string(ovfh.Mmap_handle[foot_start:])
		if check_ovfh_footer(who, ov_footer) {
			if DEBUG_OV {
				log.Printf("%s OK Read_Foot_ov -> check_ovfh_footer", who)
			}
			return ov_footer, nil
		} else {
			return "", fmt.Errorf("%s ERROR Read_Foot_ov -> check_ovfh_footer", who)
		}
	}
	return "", fmt.Errorf("%s ERROR Read_Foot_ov mmap_size=%d 'foot_start=%d < OV_RESERVE_END=%d'", who, ovfh.Mmap_size, foot_start, OV_RESERVE_END)
} // end func Read_Foot_ov

func Create_ov(who string, File_path string, hash string, pages int) error {
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
	ov_body := zerofill_block(pages, "4K") // initial max of overview data, will grow when needed
	ov_footer := zerofill(foot_str, OV_RESERVE_END)

	wb, wbt := 0, 0 // debugs written bytes

	if wb, err = init_file(who, File_path, ov_header, false); err != nil {
		log.Printf("%s ERROR Create_ov init_file ov_header fp='%s' err='%v'", who, File_path, err)
		return err
	}
	wbt += wb

	if wb, err = init_file(who, File_path, ov_body, false); err != nil {
		log.Printf("%s ERROR Create_ov init_file ov_body fp='%s' err='%v'", who, File_path, err)
		return err
	}
	wbt += wb

	if wb, err = init_file(who, File_path, ov_footer, false); err != nil {
		log.Printf("%s ERROR Create_ov init_file ov_footer fp='%s' err='%v'", who, File_path, err)
		return err
	}
	wbt += wb

	if DEBUG_OV {
		log.Printf("%s Create_ov OK fp='%s' wbt=%d", who, File_path, wbt)
	}
	return nil

} // end func Create_ov

func Grow_ov(who string, ovfh *OVFH, pages int, blocksize string, mode int, delete bool) (*OVFH, error) {
	var err error
	var errstr string
	var header string
	var footer string
	var wbt int

	if DEBUG_OV {
		log.Printf("%s Grow_ov pages=%d bs=%s fp='%s'", who, pages, blocksize, ovfh.File_path)
	}

	if mode != 999 { // dont do these checks if we want to fix overview footer

		// update footer
		if _, err := Update_Footer(who, ovfh, "Grow_ov"); err != nil {
			return nil, err
		}

		// check header
		if header, err = Read_Head_ov(who, ovfh); err != nil {
			return nil, err
		}
		if retbool := check_ovfh_header(who, header); !retbool {
			err = fmt.Errorf("%s ERROR Grow_ov -> check_ovfh_header fp='%s' header='%s' retbool=false", who, ovfh.File_path, header)
			log.Printf("%s ERROR Grow_ov -> check_ovfh_header err='%v'", who, err)
			return nil, err
		}
		if DEBUG_OV {
			log.Printf("%s Grow_ov fp='%s' check_ovfh_header OK Findex=%d", who, ovfh.File_path, ovfh.Findex)
		}

		// check footer
		if footer, err = Read_Foot_ov(who, ovfh); err != nil {
			log.Printf("%s ERROR Grow_ov -> Read_Foot_ov err='%v'", who, err)
			return nil, err
		}
		if retbool := check_ovfh_footer(who, footer); !retbool {
			err = fmt.Errorf("%s ERROR Grow_ov -> check_ovfh_footer fp='%s' footer='%s' retbool=false", who, ovfh.File_path, footer)
			log.Printf("%s ERROR Grow_ov -> check_ovfh_footer err='%v'", who, err)
			return nil, err
		}
		if DEBUG_OV {
			log.Printf("%s Grow_ov fp='%s' check_ovfh_footer OK Findex=%d", who, ovfh.File_path, ovfh.Findex)
		}

		// 1. overwrite footer area while still mapped
		delete = false
		if _, err, errstr = Write_ov(who, ovfh, "", false, true, true, delete); err != nil {
			log.Printf("%s ERROR Grow_ov -> Write_ov err='%v' errstr='%s'", who, err, errstr)
			return nil, err
		}

	} // end if mode != 999

	/*
		if mode == 999 {
			// 1. overwrite footer area while still mapped
			if _, err, errstr = Write_ov(who, ovfh, "", false, true, true, delete); err != nil {
				log.Printf("%s ERROR Grow_ov -> Write_ov err='%v' errstr='%s' mode=%d", who, err, errstr, mode)
				return nil, err
			}
		}*/

	// 2. unmap and close overview mmap
	force_close := true
	if err = handle_close_ov(who, ovfh, false, false, force_close); err != nil {
		return nil, err
	}
	if DEBUG_OV {
		log.Printf("%s Grow_ov -> 2. mmap closed OK fp='%s' ", who, ovfh.File_path)
	}

	// 3. extend the overview body
	if wb, err := init_file(who, ovfh.File_path, zerofill_block(pages, blocksize), true); err != nil {
		log.Printf("%s ERROR Grow_ov -> init_file1 err='%v'", who, err)
		return nil, err
	} else {
		wbt += wb
	}
	if DEBUG_OV {
		log.Printf("%s Grow_ov fp='%s' zerofill_block=%d OK", who, ovfh.File_path, wbt)
	}
	if delete {
		// 3.1 reopen mmap file
		ovfh.File_handle, ovfh.Mmap_handle, err = utils.MMAP_FILE(ovfh.File_path, "r")
		if err != nil || ovfh == nil {
			log.Printf("%s ERROR Grow_ov -> 3.1 handle_open_ov new_ovfh='%v' err='%v'", who, ovfh, err)
			return nil, err
		}
		if ovfh.Mmap_handle == nil {
			err = fmt.Errorf("%s ERROR Grow_ov -> 3.1 handle_open_ov Mmap_handle=nil", who)
			log.Printf("%s", err)
			return nil, err
		}
		log.Printf("3.1 reopen mmap file oldMmap_size=%d newMmap_size=%d", ovfh.Mmap_size, len(ovfh.Mmap_handle))
		ovfh.Mmap_size = len(ovfh.Mmap_handle)
		ovfh.Mmap_range = ovfh.Mmap_size - 1
		// 3.2 unmap and close overview mmap
		retbool, err := utils.MMAP_CLOSE(ovfh.File_path, ovfh.File_handle, ovfh.Mmap_handle, "r")
		if !retbool || err != nil {
			log.Printf("%s Error Grow_ov -> 3.2 MMAP_CLOSE retbool=%t err='%v' fp='%s' ", who, retbool, err, ovfh.File_path)
			return nil, err
		}
		if DEBUG_OV {
			log.Printf("%s Grow_ov -> 3.2 mmap closed OK fp='%s' ", who, ovfh.File_path)
		}
		ovfh.Mmap_size += OV_RESERVE_END
	}

	// 4. append footer
	ov_footer := construct_footer(who, ovfh, "Grow_ov()")
	if len(ov_footer) != OV_RESERVE_END {
		log.Printf("%s Error Grow_ov -> 4. ov_footer=%d != OV_RESERVE_END=%d", len(ov_footer), OV_RESERVE_END)
		return nil, err
	}
	if wb, err := init_file(who, ovfh.File_path, ov_footer, true); err != nil {
		log.Printf("%s ERROR Grow_ov -> init_file2 err='%v'", who, err)
		return nil, err
	} else {
		wbt += wb
	}
	// footer appended

	// 5. reopen mmap file
	new_ovfh, err := handle_open_ov(who, ovfh.Hash, ovfh.File_path)
	if err != nil || new_ovfh == nil {
		log.Printf("%s ERROR Grow_ov -> handle_open_ov new_ovfh='%v' err='%v'", who, new_ovfh, err)
		return nil, err
	}
	if new_ovfh.Mmap_handle == nil {
		err = fmt.Errorf("%s ERROR Grow_ov -> handle_open_ov 5. Mmap_handle=nil", who)
		log.Printf("%s", err)
		return nil, err
	}
	// 6. done
	body_end := new_ovfh.Mmap_size - OV_RESERVE_END
	if DEBUG_OV {
		log.Printf("%s Grow_ov OK fp='%s' wbt=%d body_end=%d Findex=%d", who, new_ovfh.File_path, wbt, body_end, new_ovfh.Findex)
	}
	return new_ovfh, err
} // end func Grow_ov

func Update_Footer(who string, ovfh *OVFH, src string) (*OVFH, error) {
	if ovfh.Findex == 0 || ovfh.Last == 0 || ovfh.Mmap_handle == nil {
		return nil, fmt.Errorf("%s ERROR Update_Footer ovfh.Findex=%d ovfh.Last=%d src=%s ovfh.Mmap_handle=%d src=%s", who, ovfh.Findex, ovfh.Last, src, len(ovfh.Mmap_handle), src)
	}
	if DEBUG_OV {
		log.Printf("%s Update_Footer ovfh.Findex=%d ovfh.Last=%d src=%s", who, ovfh.Findex, ovfh.Last, src)
	}
	ov_footer := construct_footer(who, ovfh, "Update_Footer()")
	new_ovfh, err, _ := Write_ov(who, ovfh, ov_footer, false, true, false, false)
	if err != nil {
		log.Printf("%s ERROR Update_Footer -> Write_ov err='%v' src=%s", who, err, src)
	} else {
		if DEBUG_OV {
			log.Printf("%s OK Update_Footer -> Write_ov len_ov_footer=%d src=%s", who, len(ov_footer), src)
		}
	}
	return new_ovfh, err
} // end func Update_Footer

// private overview functions

func construct_footer(who string, ovfh *OVFH, src string) string {
	bodyend := ovfh.Mmap_size - OV_RESERVE_END
	foot_str := fmt.Sprintf("%s%d,last=%d,Findex=%d,bodyend=%d,fend=%d,zeropad=%s,%s", FOOTER_BEG, utils.Nano(), ovfh.Last, ovfh.Findex, bodyend, bodyend+OV_RESERVE_END, ZERO_PATTERN, FOOTER_END)
	if DEBUG_OV {
		log.Printf("%s | construct_footer foot_str='%s' src=%s", who, foot_str, src)
	}
	ov_footer := zerofill(foot_str, OV_RESERVE_END)
	return ov_footer
} // end func construct_footer

func check_ovfh_header(who string, header string) bool {
	if strings.HasPrefix(header, HEADER_BEG) {
		if strings.HasSuffix(header, HEADER_END) {
			return true
		} else {
			log.Printf("%s ERROR check_ovfh_header !HasSuffix", who)
		}
	} else {
		log.Printf("%s ERROR check_ovfh_header !HasPrefix", who)
	}
	return false
} // end func check_ovfh_header

func check_ovfh_footer(who string, footer string) bool {
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
			log.Printf("%s ERROR check_ovfh_footer !HasSuffix", who)
		}

	} else {
		log.Printf("%s ERROR check_ovfh_footer !HasPrefix", who)
	}
	return false
} // end func check_ovfh_footer

func init_file(who string, File_path string, data string, grow bool) (int, error) {
	DEBUG_OV1 := true
	if DEBUG_OV {
		log.Printf("%s init_file fp='%s' len_data=%d grow=%t", who, File_path, len(data), grow)
	}
	var fh *os.File
	var err error
	var wb int
	if fh, err = os.OpenFile(File_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		defer fh.Close()
		w := bufio.NewWriter(fh)
		if wb, err = w.WriteString(data); err == nil {
			if err = w.Flush(); err == nil {
				if DEBUG_OV1 {
					log.Printf("%s init_file wrote fp='%s' len_data=%d wb=%d grow=%t", who, File_path, len(data), wb, grow)
				}
				return wb, nil
			}
		}
	}
	log.Printf("%s ERROR init_file err='%v'", who, err)
	return wb, err
} // end func init_file

func preload_zero(what string) {
	if what == "PRELOAD_ZERO_1K" && PRELOAD_ZERO_1K == "" {
		PRELOAD_ZERO_1K = zerofill(ZERO_PATTERN, 1024)
		//log.Printf("OV PRELOAD_ZERO_1K=%d", len(PRELOAD_ZERO_1K))
	} else if what == "PRELOAD_ZERO_4K" && PRELOAD_ZERO_4K == "" {
		PRELOAD_ZERO_4K = zerofill_block(4, "1K")
		//log.Printf("OV PRELOAD_ZERO_4K=%d", len(PRELOAD_ZERO_4K))
	} else if what == "PRELOAD_ZERO_128K" && PRELOAD_ZERO_128K == "" {
		PRELOAD_ZERO_128K = zerofill_block(32, "4K")
		//log.Printf("OV PRELOAD_ZERO_128K=%d", len(PRELOAD_ZERO_128K))
	} else if what == "PRELOAD_ZERO_1M" && PRELOAD_ZERO_1M == "" {
		PRELOAD_ZERO_1M = zerofill_block(8, "128K")
		//log.Printf("OV PRELOAD_ZERO_1M=%d", len(PRELOAD_ZERO_1M))
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

func Scan_Overview(file string, group string, a uint64, b uint64, fields string, conn net.Conn, initline string, txb *int) ([]string, error) {
	if file == "" {
		return nil, fmt.Errorf("Error Scan_Overview file=nil||a=nil||b=nil")
	}
	var offset int64

	if group != "" {
		index := fmt.Sprintf("%s.Index", file)
		offset = OVIndex.ReadOverviewIndex(index, group, a, b)
	}

	if a < 1 {
		return nil, fmt.Errorf("Error Scan_Overview a < 1")
	}
	//to_end := false
	if b == 0 {
		//to_end = true
	} else if a > b {
		return nil, fmt.Errorf("Error Scan_Overview a=%d > b=%d", a, b)
	}
	if fields == "" {
		fields = "all"
	}

	var lines []string
	readFile, err := os.Open(file)
	if err != nil {
		log.Printf("Error Scan_Overview os.Open fp='%s' err='%v'", filepath.Base(file), err)
		return lines, err
	}
	defer readFile.Close()
	if offset > 0 {
		_, err = readFile.Seek(offset, 0)
		if err != nil {
			log.Printf("Error Scan_Overview os.Open.Seek fp='%s' offset=%d err='%v'", offset, filepath.Base(file), err)
			return nil, err
		}
		log.Printf("Scan_Overview SEEK fp='%s' a=%d @offset=%d", filepath.Base(file), a, offset)
	}

	fileScanner := bufio.NewScanner(readFile)
	maxScan := 4096 // default NewScanner uses 64K buffer
	if fields == "ReOrderOV" {
		maxScan = 1024 * 1024
	}
	buf := make([]byte, maxScan)
	fileScanner.Buffer(buf, maxScan)
	fileScanner.Split(bufio.ScanLines)
	var lc uint64 // linecounter
	offsets := make(map[uint64]int64)
	msgnums := []uint64{}
	//log.Printf("Scan_Overview fp='%s' a=%d b=%d maxScan=%d", filepath.Base(file), a, b, maxScan)

	if conn != nil {
		if initline != "" {
			// conn is set: send init line
			if err := sendlineOV(initline+CRLF, conn, txb); err != nil {
				return nil, err
			}
		}
	}

	var msgnum uint64
forfilescanner:
	for fileScanner.Scan() {
		line := fileScanner.Text()
		if fields == "ReOrderOV" {
			lines = append(lines, line)
			lc++
			continue forfilescanner
		}
		if offset <= 0 && lc == 0 {
			// pass, ignore first line aka linecount 0: header from overview file
			lc++
			//if offset <OV_RESERVE_BEG
			offset += OV_RESERVE_BEG
			continue forfilescanner
		}
		lc++
		ll := len(line)
		if ll == 0 {
			err = fmt.Errorf("break Scan_Overview lc=%d got empty line?!")
			log.Printf("%s", err)
			return nil, err
		}

		if ll >= 0 && ll <= 3 {
			if line == "" || line[0] == 0 || line[0] == '\x00' || line == "EOV" {
				log.Printf("break Scan_Overview lc=%d")
				break forfilescanner
			}
		}

		datafields := strings.Split(line, "\t")
		if len(datafields) != OVERVIEW_FIELDS {
			err = fmt.Errorf("Error Scan_Overview lc=%d len(fields)=%d != OVERVIEW_FIELDS=%d file='%s' line='%s'", lc, len(datafields), OVERVIEW_FIELDS, filepath.Base(file), line)
			log.Printf("%s", err)
			//return nil, err
			break forfilescanner
		}

		msgnum = utils.Str2uint64(datafields[0])
		if msgnum == 0 {
			if len(datafields[4]) > 0 && (datafields[4][0] == 'X' || datafields[4][0] == 0) { // check if first char is X or NUL
				// expiration removed article from overview
				continue forfilescanner
			}
			err = fmt.Errorf("Error Scan_Overview lc=%d msgnum=0 file='%s'", lc, filepath.Base(file))
			log.Printf("%s", err)
			return nil, err
		}

		if fields == "NewOVI" {
			offsets[msgnum] = offset
			offset += int64(ll) + 1 // + 1 == int64(len(LF))
			msgnums = append(msgnums, msgnum)
			continue forfilescanner
		}

		if msgnum < a {
			// msgnum is not in range
			//log.Printf("Scan.Overview msgnum=%d < a=%d lc=%d", msgnum, *a, lc)
			continue forfilescanner
		}

		if !isvalidmsgid(datafields[4], true) {
			if len(datafields[4]) > 0 && (datafields[4][0] == 'X' || datafields[4][0] == 0) { // check if first char is X or NUL
				// expiration removed article from overview
				continue forfilescanner
			}
			log.Printf("Error Scan_Overview file='%s' lc=%d field[4] err='!isvalidmsgid'", filepath.Base(file), lc)
			break
		}
		// view-filter
		if fields == "all" && strings.HasSuffix(datafields[4], "googlegroups.com>") {
			continue forfilescanner
		}

		/*
			0 "NUM",
			1 "Subject:",
			2 "From:",
			3 "Date:",
			4 "Message-ID:",
			5 "References:",
			6 "Bytes:",
			7 "Lines:",
			8 "Xref:full",
		*/

		switch fields {
		case "LISTGROUP":
			line := fmt.Sprintf("%d", msgnum)
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "XHDR SUBJECT":
			line := fmt.Sprintf("%d %s", msgnum, datafields[1])
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "XHDR FROM":
			line := fmt.Sprintf("%d %s", msgnum, datafields[2])
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "XHDR DATE":
			line := fmt.Sprintf("%d %s", msgnum, datafields[3])
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "XHDR MESSAGE-ID":
			line := fmt.Sprintf("%d %s", msgnum, datafields[4])
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "XHDR REFERENCES":
			line := fmt.Sprintf("%d %s", msgnum, datafields[5])
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "XHDR BYTES":
			line := fmt.Sprintf("%d %s", msgnum, datafields[6])
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "XHDR LINES":
			line := fmt.Sprintf("%d %s", msgnum, datafields[7])
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "XHDR XREF":
			line := fmt.Sprintf("%d %s", msgnum, datafields[8])
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "all":
			if conn == nil {
				lines = append(lines, line)
			} else {
				if err := sendlineOV(line+CRLF, conn, txb); err != nil {
					return nil, err
				}
			}
		case "msgid":
			lines = append(lines, datafields[4]) // catches message-id field
			log.Printf("Scan_Overview returns a=%d b=%d file='%s' msgid='%s'", a, b, filepath.Base(file), datafields[4])
			break forfilescanner
		default:
			log.Printf("Error scan_overview unknown *fields=%s", fields)
			break forfilescanner
		}

		if msgnum >= b {
			break forfilescanner
		}

	} // end for filescanner

	if fields == "NewOVI" {
		indexSize := 100 // gets offsets for every 100 overview entries
		tmp_offsets := make(map[uint64]int64, indexSize)
		tmp_msgnums := []uint64{}
		for _, msgnum := range msgnums {
			if len(tmp_offsets) >= indexSize {
				WriteOverviewIndex(file, tmp_msgnums, tmp_offsets)
				tmp_msgnums = nil
				tmp_offsets = make(map[uint64]int64, indexSize)
			}
			tmp_msgnums = append(tmp_msgnums, msgnum)
			tmp_offsets[msgnum] = offsets[msgnum]
		}
		if len(tmp_offsets) > 0 && len(tmp_msgnums) == len(tmp_offsets) {
			WriteOverviewIndex(file, tmp_msgnums, tmp_offsets)
		}
		return nil, nil
	} // end NewINDEX

	sendlineOV(DOTCRLF, conn, txb)

	return lines, err
} // end func Scan_Overview

func sendlineOV(line string, conn net.Conn, txb *int) error {
	if line == "" {
		return fmt.Errorf("Error OV sendline line=nil")
	}
	if conn != nil {
		tx, err := io.WriteString(conn, line)
		if err != nil {
			return err
		}
		if txb != nil {
			*txb += tx
		}
	}
	return nil
} // end func sendline

func ParseHeaderKeys(head *[]string, laxmid bool) (headermap map[string][]string, keysorder []string, msgid string, err error) {
	/*  RFC 5322 section 2.2:
	Header fields are lines beginning with a field name, followed by a
	colon (":"), followed by a field body, and terminated by CRLF.
	*
	* A field name MUST be composed of printable US-ASCII characters (i.e.,
	characters that have values between 33 and 126, inclusive), except
	colon.
	*
	* A field body may be composed of printable US-ASCII characters
	as well as the space (SP, ASCII value 32) and horizontal tab (HTAB,
	ASCII value 9) characters (together known as the white space
	characters, WSP).  A field body MUST NOT include CR and LF except
	when used in "folding" and "unfolding", as described in section
	2.2.3.  All field bodies MUST conform to the syntax described in
	sections 3 and 4 of this specification.
	*/
	if head == nil {
		return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: head=nil")
	}
	key := ""
	//msgid = ""
	headermap = make(map[string][]string)
	keysorder = []string{}
	for i, line := range *head {
		if len(line) < 2 {
			return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: Header attribute expected i=%d head=%d line='%s'", i, len(*head), line)
		}
		if isspace(line[0]) && key != "" {
			headermap[key] = append(headermap[key], line)
			continue
		} else if isspace(line[0]) && key == "" {
			return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: Unexpected continuation of a header somehow missed line='%s' i=%d head=%d key='%s'", line, i, len(*head), key)
		}
		k := strings.Index(line, ":")
		if k < 1 {
			return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: Colon not found in expected 'key: value' syntax | line='%s'", line)
		}

		key = string(line[0:k])
		value := string(line[k+1:])

		for j, c := range key {
			if c < 33 || c > 126 {
				return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: Key 'c < 32 || c > 126' key='%s' line='%s' i=%d j=%d", key, line, i, j)
			}
		}
		/*
			for j, c := range value {
				if c < 32 && c != 9 {
					return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: invalid value char line='%s' i=%d j=%d c=%d", line, i, j, c)
				}
			}
		*/
		headermap[key] = append(headermap[key], value)
		keysorder = append(keysorder, key)
	}
	msgid_key := ""
	for key, values := range headermap {
		if strings.ToLower(key) == "message-id" {
			msgid = strings.Join(values, "")
			msgid_key = key
			if msgid != "" {
				msgid, err = GetMessageID(msgid, laxmid)
				if err != nil || msgid == "" {
					return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys getMessageID key='%s' val='%v'", key, values)
				}
				break
			}
		}
	}
	if msgid != "" && len(headermap[msgid_key]) > 1 {
		headermap[msgid_key] = nil
		headermap[msgid_key] = append(headermap[msgid_key], msgid)
	}
	//_ = ConstructHeader(headermap, &keysorder)
	//log.Printf("ParseHeaderKeys: returns map=%d ord=%d \n v='%v'\n", len(headermap), len(keysorder), headermap)
	return
} // end func ParseHeaderKeys

func ConstructHeader(headermap map[string][]string, keysorder *[]string) (headlines *[]string) {
	var head []string
	var lineBegin bool
	if keysorder == nil {
		return nil
	}
	lc := 0
	for _, akey := range *keysorder {
		lineBegin = true
		line := akey + ": "
		for _, value := range headermap[akey] {
			if lineBegin {
				head = append(head, line+value)
				lineBegin = false
				lc++
			} else {
				if len(value) > 0 && utils.IsSpace(value[0]) {
					head = append(head, value)
					lc++
				}
			}
		} // end for
	} // end for
	headlines = &head
	//log.Printf("ConstructHeader returns lc=%d headlines=%d", lc, len(head))
	return
} // end func ConstructHeader

func GetMessageID(amsgid string, laxmid bool) (string, error) {
	//lastInd := -1
	msgid := strings.TrimSpace(amsgid)
	containsSpace := strings.Contains(msgid, " ")
	//containsAT := strings.Contains(msgid, "@")

	if laxmid && containsSpace {
		if strings.HasPrefix(msgid, "<") && strings.HasSuffix(msgid, ">") {
			return msgid, nil
		}
	} else if !laxmid && containsSpace {
		return "", fmt.Errorf("Error GetMessageID msgid='%s' laxmid=%t containsSpace=%t", msgid, laxmid, containsSpace)
	}

	if laxmid && strings.HasSuffix(msgid, ">#1/1") {
		lastInd := strings.LastIndex(msgid, "#")
		if lastInd > 0 {
			msgid = string(msgid[:lastInd])
		}
	} else if laxmid && strings.HasPrefix(msgid, "<") && (strings.Contains(msgid, ">?(") || strings.Contains(msgid, "> (") || strings.Contains(msgid, ">(UMass-") || strings.Contains(msgid, ">-(UMass-")) && strings.HasSuffix(msgid, ")") {
		split := strings.Split(msgid, ">")
		msgid = split[0] + ">"
	}

	if laxmid {
		if strings.HasPrefix(msgid, "<") && strings.HasSuffix(msgid, ">") {
			return msgid, nil
		}
	} else if !laxmid {
		if strings.HasPrefix(msgid, "<") && strings.Contains("msgid", "@") && strings.HasSuffix(msgid, ">") {
			return msgid, nil
		}
	}
	return "", fmt.Errorf("ERROR: getMessageID failed amsgid='%s'", amsgid)
} // end func GetMessageID

func ParseDate(dv string) (unixepoch int64, err error) {
	debug := false
	if dv == "" {
		return 0, fmt.Errorf("Error OV ParseDate dv=nil")
	}
	var parsedTime time.Time
	dv = strings.TrimSpace(dv)
	// Try parsing with different layouts
	for _, layout := range NNTPDateLayouts {
		parsedTime, err = time.Parse(layout, dv)
		if err == nil {
			break
		}
		//log.Printf("Error OV ParseDate: dv='%s' err='%v'", *dv, err)
	}
	/* todo: stupid bruteforce need rethink? */
	if err != nil {
		if debug {
			log.Printf("WARN1 OV ParseDate: dv='%s' try extractMatchingText", dv)
		}
		/*
		for _, layout := range NNTPDateLayouts {
			parsedText := extractMatchingText(dv, layout)
			parsedTime, err = time.Parse(layout, parsedText)
			if err == nil {
				if debug {
					log.Printf("INFO1 OV ParseDate: dv='%s' parsedText='%s' parsedTime='%s' layout='%s'", dv, parsedText, parsedTime, layout)
				}
				break
			}
			//log.Printf("Error OV ParseDate: dv='%s' err='%v'", *dv, err)
		}
		*/
	}

	if err != nil {
		//reterr := fmt.Errorf("Error OV ParseDate: dv='%s' err='%v'", dv, err)
		reterr := fmt.Errorf("ParseDate: dverr='%s'", dv)
		return 0, reterr
	} else {
		// Convert the parsed time to Unix epoch timestamp
		epochTimestamp := parsedTime.Unix()
		if epochTimestamp < 0 || epochTimestamp > 0 {
			unixepoch = epochTimestamp
			//log.Printf("ParseDate dv='%s' RFC3339='%s'", *dv, parsedTime.Format(time.RFC3339))
			if debug {
				log.Printf("ParseDate dv='%s' U: %d", dv, unixepoch)
			}
		} else {
			log.Printf("Error OV ParseDate dv='%s' t='%s'epochTimestamp=%d == 0", dv, parsedTime.Format(time.RFC3339), epochTimestamp)
		}
	}
	return
} // end func ParseDate

// Function to extract only the portion of the input string that matches the layout
func extractMatchingText(input string, layout string) string {
	_, err := time.Parse(layout, input)
	if err == nil {
		return input
	}
	for i := 6; i < len(input); i++ {
		// Attempt to parse the string with the layout
		_, err := time.Parse(layout, input[:i])
		if err != nil {
			// If an error occurs, the previous substring is the matching text
			continue
		}
		return input[:i]
	}
	// If the loop completes without an error, the entire input is the matching text
	return input
} // end func extractMatchingText

func isspace(b byte) bool {
	return b < 33
}

func ConnSQL(username string, password string, hostname string, database string) (*sql.DB, error) {
	params := "?timeout=86400s"
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s%s", username, password, "tcp", hostname, database, params)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("ERROR overview.ConnSQL 'open' failed err='%v'", err)
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		log.Printf("ERROR overview.ConnSQL 'ping' failed err='%v'", err)
		return nil, err
	}
	return db, nil
} // end func connSQL

func MsgIDhash2mysql(messageidhash string, size int, db *sql.DB) (bool, error) {
	if len(messageidhash) != 64 || size == 0 {
		return false, fmt.Errorf("ERROR overview.MsgIDhash2mysql len(messageidhash) != 64 || size=%d", size)
	}
	if stmt, err := db.Prepare("INSERT INTO msgidhash (hash, fsize) VALUES (?,?)"); err != nil {
		log.Printf("ERROR overview.MsgIDhash2mysql db.Prepare() err='%v'", err)
		return false, err
	} else {
		if res, err := stmt.Exec(messageidhash, size); err != nil {
			log.Printf("ERROR overview.MsgIDhash2mysql stmt.Exec() err='%v'", err)
			return false, err
		} else {
			if rowCnt, err := res.RowsAffected(); err != nil {
				log.Printf("ERROR overview.MsgIDhash2mysql res.RowsAffected() err='%v'", err)
				return false, err
			} else {
				if rowCnt == 1 {
					return true, nil // inserted
				}
				return false, nil // duplicate
			}
		}
	}
	return false, fmt.Errorf("ERROR overview.MsgIDhash2mysql() uncatched return")
} // end func MsgIDhash2mysql

func IsMsgidHashSQL(messageidhash string, db *sql.DB) (bool, bool, error) {

	if len(messageidhash) != 64 {
		return false, false, fmt.Errorf("ERROR overview.IsMsgidHashSQL len(messageidhash) != 64")
	}
	var hash string
	var stat string
	if err := db.QueryRow("SELECT hash,stat FROM msgidhash WHERE hash = ? LIMIT 1", messageidhash).Scan(&hash,&stat); err != nil {
		if err == sql.ErrNoRows {
			return false, false, nil
		}
		log.Printf("ERROR overview.IsMsgidHashSQL err='%v'", err)
		return false, false, err
	}
	if hash == messageidhash {
		var drop bool
		if len(stat) == 1 {
			drop = true
		}
		/*
		switch stat {
			case "c":
				// cancelled
				drop = true
			case "d":
				// dmca
				drop = true
			case "f":
				// filtered
				drop = true
			case "n":
				// nocem
				drop = true
			case "r":
				// removed
				drop = true
		}
		*/
		return true, drop, nil
	}
	return false, false, fmt.Errorf("ERROR overview.IsMsgidHashSQL() uncatched return")
} // end func IsMsgidHashSQL
