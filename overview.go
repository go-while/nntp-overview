package overview


import (
    "bufio"
    //"crypto/sha256"
    //"encoding/hex"
    //"encoding/binary"
    "fmt"
    "log"
    "os"
    "unicode"
    //"strconv"
    "path/filepath"
    "strings"
    "time"
    "sync"
    "github.com/edsrzf/mmap-go"
    "github.com/go-while/go-utils"
)


const (
    DEBUG_OV bool = false
    null string = "\x00"
    tab string = "\010"
    CR string = "\r"
    LF string = "\n"
    CRLF string = CR+LF

    XREF_PREFIX = "nntp"

    MAX_FLUSH int64 = 15            // flush mmaps every N seconds

    LIMIT_SPLITMAX_NEWSGROUPS int = 25

    SIZEOF_FOOT int = 7
    OVL_CHECKSUM int = 5
    OVERVIEW_FIELDS int = 9
    OVERVIEW_TABS int = OVERVIEW_FIELDS-1

    ZERO_PATTERN string = "zerofill"        // replace this pattern with zero padding
    ZERO_FILL_STR string = null             // zero pad with nul
    FILE_DELIM string = "\n"                // string used as file delimiter

    OV_RESERVE_BEG uint64 = 128                // initially reserve n bytes in overview file
    OV_RESERVE_END uint64 = 128                // finally reserve n bytes in overview file
    OV_LIMIT_MAX_LINE int = 1024            // limit stored overview line length //FIXME TODO

    //OV_FLUSH_EVERY_BYTES int = 128*1024     // flush memory mapped overview file every 128K (zfs recordsize?)     //FIXME TODO
    //OV_FLUSH_EVERY_SECS int64 = 60          // flush memory mapped overview file every 60s                        //FIXME TODO

    HEADER_BEG string = string("#ov_init=")
    HEADER_END string = string("EOH"+FILE_DELIM)
    BODY_END string = string(FILE_DELIM+"EOV"+FILE_DELIM)
    FOOTER_BEG string = string(BODY_END+"time=")
    FOOTER_END string = string(FILE_DELIM+"EOF"+FILE_DELIM)
    ZERO_PATTERN_LEN int = len(ZERO_PATTERN)        // the len of the pattern
    ERR_OV_OVERFLOW string = "ef01"                 // write overview throws errstr 'ef01' when buffer is full and needs to grow
)

var (
    PRELOAD_ZERO_1K         string
    PRELOAD_ZERO_4K         string
    PRELOAD_ZERO_128K       string
    PRELOAD_ZERO_1M         string
    open_mmap_overviews         Open_MMAP_Overviews
    Known_msgids                Known_MessageIDs
    OV_handler                  OV_Handler
    open_request_chan           chan Overview_Open_Request
    close_request_chan          chan Overview_Close_Request
    //read_request_chan         chan Overview_Read_Request  // FIXME TODO
    workers_done_chan           chan int   // holds an empty struct for done overview worker
    count_open_overviews        chan struct{}   // holds an empty struct for every open overview file
    MAX_Open_overviews_chan     chan struct{}   // locking queue prevents opening of more overview files
)

type Overview_Open_Request struct {
    hash        string                      // hash from group (hash.overview file)
    file_path   string                      // grouphash.overview file path
    reply_chan  chan Overview_Reply         // replies back with the 'OVFH' or err
}

type Overview_Close_Request struct {
    force_close     bool                    // will be set to true if this overview has to be closed
    ovfh            OVFH                    // overview mmap file handle struct
    reply_chan      chan Overview_Reply     // replies back with the 'OVFH' or err
}

type Overview_Reply struct {
    ovfh        OVFH                        // overview mmap file handle struct
    err         error                       // return error
    retbool     bool                        // returns a bool
}

type OVFH struct {
    File_path       string
    File_handle     *os.File
    Mmap_handle     mmap.MMap
    Mmap_size       uint64
    Mmap_range      uint64
    Time_open       int64
    Time_flush      int64
    Written         uint64
    Findex          uint64
    Last            uint64
    Hash            string
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
    Subject         string
    From            string
    Date            string
    Messageid       string
    References      []string
    Bytes           int
    Lines           int
    Xref            string
    Newsgroups      []string
    Checksum        int // has to match OVL_CHECKSUM
    Retchan         chan []ReturnChannelData
    ReaderCachedir  string
}

type ReturnChannelData struct {
    Retbool     bool
    Msgnum      uint64
    Newsgroup   string
}

func Load_Overview(maxworkers int, max_queue_size int, max_open_mmaps int, known_messageids int, ov_opener int, ov_closer int, stop_server_chan chan bool, debug_OV_handler bool) (chan OVL) {
    if stop_server_chan == nil {
        log.Printf("ERROR Load_Overview stop_server_chan=nil")
        return nil
    }
    if max_open_mmaps < 2 {
        max_open_mmaps = 2
    }
 /* Load_Overview has to be called with
     *
     * 1) maxworkers int        # number of workers to process incoming headers (recommended: = CPUs*2)
     *
     * 2) max_queue_size int    # limit the incoming queue (recommended: = maxworkers*2)
     *
     * 3) max_open_mmaps int    # limit max open memory mappings (recommended: = 4-768 tested)
     *
     * 4) max_known_messageids int  # cache known messageids. should be queue length of your app
     *
     * 5) ov_opener int         # number of handlers for open_requests (recommended: = maxworkers*2)
     *
     * 6) ov_closer int         # number of handlers for close_requests (recommended: = maxworkers*2)
     *
     * 7) a stop_server_chan := make(chan bool, 1)
     *   so we can send a true to the channel from our app and the worker(s) will close
     *
     * X) the return value of Load_Overview() -> Launch_overview_workers() is the input_channel: 'overview_input_channel'
     *      that's used to feed extracted ovl (OVL overview lines) to workers
     */

    if MAX_Open_overviews_chan == nil {
        preload_zero("PRELOAD_ZERO_1K")
        preload_zero("PRELOAD_ZERO_4K")
        preload_zero("PRELOAD_ZERO_128K")
        // 'open_mmap_overviews' locks overview files per newsgroup in GO_process_incoming_overview()
        // so concurrently running overview workers will not write at the same time to same overview/newsgroup
        // if a worker 'A' tries to write to a mmap thats already open by any other worker at this moment
        // worker A creates a channel in 'ch' and waits for the other worker to respond back over that channel
        // v: holds a true with "group" as key if overview is already open
        // ch: stores the channels from worker A with "group" as key, so the other worker holding the map
        //      will signal to that channel its unlocked
        open_mmap_overviews = Open_MMAP_Overviews {
                v: make(map[string]int64, max_open_mmaps),
                ch: make(map[string][]chan struct{}, max_open_mmaps),
            }

        if known_messageids > 0 { // setup known_messageids map only if we want to
            Known_msgids = Known_MessageIDs {
                v: make(map[string]bool, known_messageids),
                Debug: debug_OV_handler,
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
        if DEBUG_OV {
            debug_OV_handler = true
        }
        OV_handler = OV_Handler {
            V: make(map[string]OV_Handler_data, max_open_mmaps),
            Debug: debug_OV_handler,
            STOP: stop_server_chan,
            MAX_OPEN_MMAPS: max_open_mmaps,
        }

        log.Printf("Load_Overview maxworkers=%d, max_queue_size=%d, max_open_mmaps=%d, known_messageids=%d, ov_opener=%d, ov_closer=%d, stop_server_chan='%v' debug_OV_handler=%t len(MAX_Open_overviews_chan)=%d",
                                    maxworkers, max_queue_size, max_open_mmaps, known_messageids, ov_opener, ov_closer, stop_server_chan, debug_OV_handler, len(MAX_Open_overviews_chan) )
        time.Sleep(2 * time.Second)
        return Launch_overview_workers(maxworkers, max_queue_size, max_open_mmaps, ov_opener, ov_closer, stop_server_chan)
    } else {
        log.Printf("ERROR: Load_Overview already created")
    }

    return nil
} // end func Load_Overview


func Launch_overview_workers(maxworkers int, max_queue_size int, max_open_mmaps int, ov_opener int, ov_closer int, stop_server_chan chan bool) (chan OVL) {
    log.Printf("Launch_overview_workers maxworkers=%d max_queue_size=%d max_open_mmaps=%d ov_opener=%d ov_closer=%d", maxworkers, max_queue_size, max_open_mmaps, ov_opener, ov_closer)

    workers_done_chan = make(chan int, maxworkers)
    overview_input_channel := make(chan OVL, max_queue_size) // one input_channel to serve them all with cap of max_queue_size

    // launch the overview worker routines
    for ov_wid := 1; ov_wid <= maxworkers; ov_wid++ {
        go overview_Worker(ov_wid, overview_input_channel, stop_server_chan)
    }

    // launch overview opener routines
    if maxworkers > 0 && ov_opener < maxworkers { ov_opener = maxworkers }
    for ov_hid := 1; ov_hid <= ov_opener; ov_hid++ {
        go OV_handler.Overview_handler_OPENER(ov_hid, open_request_chan)
    }

    // launch overview closer routines
    if maxworkers > 0 && ov_closer < maxworkers { ov_opener = maxworkers }
    for ov_hid := 1; ov_hid <= ov_closer; ov_hid++ {
        go OV_handler.Overview_handler_CLOSER(ov_hid, close_request_chan)
    }

    // launch mmaps idle check
    go OV_handler.Check_idle()

    return overview_input_channel // return overview_input_channel to app
} // end func Launch_Overview_Workers


func Watch_overview_Workers(maxworkers int, overview_input_channel chan OVL) {
    // run Watch_overview_Workers() whenever you're done feeding, before closing your app
    logstr := ""
    forever:
    for {
        open_overviews := OV_handler.open_overviews()       // int
        workers_done := len(workers_done_chan)              // int
        all_workers_done := workers_done == maxworkers      // bool
        queued_overviews := len(overview_input_channel)
        logstr = fmt.Sprintf("workers_done=%d all_workers_done=%t open_overviews=%d queued_overviews=%d", workers_done, all_workers_done, open_overviews, queued_overviews)
        if workers_done == maxworkers && all_workers_done && open_overviews == 0 && queued_overviews == 0 {
            break forever
        }
        time.Sleep(1 * time.Second)
        log.Printf("WAIT Watch_overview_Workers %s", logstr)
    }
    log.Printf("DONE Watch_overview_Workers %s", logstr)

} // end func watch_overview_Workers


func overview_Worker(ov_wid int, input_channel chan OVL, stop_server_chan chan bool) {
    log.Printf("overview_Worker %d) starting", ov_wid)

    /* the overview worker has to be called:
     *  with a worker-id >= 1
     *  input_channel := make(chan OVL, N)          // should be 1 chan global for all workers with some queue length
     *  worker_done_chan := make(chan int, 1)
     *  and dont forget the stop_server_chan
     */
    defer func(){
        log.Printf("overview_Worker %d) worker done", ov_wid)
        workers_done_chan <- ov_wid
    }()


    forever:
    for {
        select {
            case ovl, ok := <- input_channel:
                if !ok {
                    log.Printf("overview_Worker %d) input_channel is closed", ov_wid)
                    break forever
                }
                if DEBUG_OV { log.Printf("overview_Worker %d) got ovl msgid='%s'", ov_wid, ovl.Messageid) }
                // handle incoming overview line
                // loop over the newsgroups and pass the ovl to every group
                ovl.Retchan <- divide_incoming_overview(ovl) // passes XXXXX to the app via the supplied Retchan
                close(ovl.Retchan)
            case _, ok := <- stop_server_chan:
                if !ok {
                    log.Printf("overview_Worker %d) received stop signal", ov_wid)
                    break forever
                }
        } // end select
    } // end for forever
} // end func Overview_Worker


func fail_retchan(hash string, newsgroup string, retchan chan ReturnChannelData) {
    retchan <- ReturnChannelData { false, 0, newsgroup }
    close(retchan)
} // end func fail_retchan


func true_retchan(hash string, newsgroup string, msgnum uint64, retchan chan ReturnChannelData) {
    retchan <- ReturnChannelData { true, msgnum, newsgroup }
    close(retchan)
} // end func true_retchan


func return_overview_lock() {
    MAX_Open_overviews_chan <- struct{}{}
} // end func return_overview_lock


func GO_process_incoming_overview(overviewline string, newsgroup string, hash string, cachedir string, retchan chan ReturnChannelData) {
    // GO_process_incoming_overview is running concurrently!
    <- MAX_Open_overviews_chan // get a global lock to limit total num of open overview files
    defer return_overview_lock() // return the lock whenever func returns
    var err error
    mmap_file_path := cachedir+"/"+hash+".overview"

    ovfh, err := Open_ov(mmap_file_path)
    if err != nil {
        log.Printf("ERROR overview.Open_ov err='%v'", err)
        fail_retchan(hash, newsgroup, retchan)
        return
    }
    if DEBUG_OV { log.Printf("overview.Open_ov ng='%s' OK", newsgroup) }

    if ovfh.Last == 0 {
        ovfh.Last = 1
    }

    xref := fmt.Sprintf(XREF_PREFIX+" %s:%d", newsgroup, ovfh.Last)
    ovl_line := fmt.Sprintf("%d\t%s\t%s\n", ovfh.Last, overviewline, xref)
    ovfh, err, errstr := Write_ov(ovfh, ovl_line, false, false, false);
    if err != nil {
        log.Printf("ERROR Write_ovfh err='%v' errstr='%s'", err, errstr)
        fail_retchan(hash, newsgroup, retchan)
        return
    } else {
        if DEBUG_OV { log.Printf("overview.Write_ov OK ng='%s' ovfh.Findex=%d ovfh.Last=%d", newsgroup, ovfh.Findex, ovfh.Last) }

        if ovfh, err = Update_Footer(ovfh); err != nil {
            log.Printf("ERROR overview.Update_Footer ng='%s' err='%v'", newsgroup, err)
            fail_retchan(hash, newsgroup, retchan)
            return
        } else {
            if DEBUG_OV { log.Printf("overview.Update_Footer OK ng='%s' ovfh.Findex=%d ovfh.Last=%d", newsgroup, ovfh.Findex, ovfh.Last) }
        }
    }
    ovfh.Last++

    // finally close the mmap
    if DEBUG_OV { log.Printf("p_i_o: Closing fp='%s'", ovfh.File_path) }
    if err := Close_ov(ovfh, true, false); err != nil {
        log.Printf("p_i_o: ERROR FINAL Close_ovfh err='%v'", err)
        fail_retchan(hash, newsgroup, retchan)
        return
    }

    true_retchan(hash, newsgroup, uint64(ovfh.Last-1), retchan)
    return
} // end func GO_process_incoming_overview


func divide_incoming_overview(ovl OVL) []ReturnChannelData {
    var retlist []ReturnChannelData
    dones := 0
    var retchans []chan ReturnChannelData
    overviewline := Construct_OVL(ovl)

    for _, newsgroup := range ovl.Newsgroups {
        retchan := make(chan ReturnChannelData, 1)
        retchans = append(retchans, retchan)
        hash := strings.ToLower(utils.Hash256(newsgroup))
        go GO_process_incoming_overview(overviewline, newsgroup, hash, ovl.ReaderCachedir, retchan)
    }

    dropchan := -1
    checked := 0
    forever:
    for {

        test_chans:
        for i := 0; i < len(retchans); i++ {
            checked++
            select {
                // will block for response on first channel, break, remove and loop again,
                // maybe we got all done
                case retdata, ok := <- retchans[i]:
                    if retdata.Retbool || !ok { // !ok chan is closed
                        dones++
                        dropchan = i
                        if retdata.Retbool {
                            // FIXME TODO need a channe here from our app
                            //  send retdata with msgnum and newsgroup back to update activitymap
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

    if dones == len(ovl.Newsgroups) {
        if DEBUG_OV { log.Printf("divide_incoming_overview checked=%d len=%d retchans=%d retlist=%d", checked, len(ovl.Newsgroups), len(retchans), len(retlist)) }
    } else {
        log.Printf("ERROR divide_incoming_overview checked=%d len=%d retchans=%d", checked, len(ovl.Newsgroups), len(retchans))
    }
    return retlist
} // end func divide_incoming_overview


func Construct_OVL(ovl OVL) string {
    // construct an overview line
    ref_len, ref_len_limit := 0, 4096
    var references string
    for i, ref := range ovl.References {
        len_ref := len(ref)
        new_ref_len := ref_len + len_ref
        if new_ref_len > ref_len_limit {
            log.Printf("ERROR Construct_OVL new_ref_len=%d msgid='%s' i=%d", new_ref_len, ovl.Messageid, i)
            break
        }
        references = references+" "+ref
        ref_len += len_ref

    }
    ret_ovl_str := fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%d\t%d", ovl.Subject, ovl.From, ovl.Date, ovl.Messageid, references, ovl.Bytes, ovl.Lines )
    return ret_ovl_str
} // end func Construct_OVL


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

        nextline := i+1
        spaced_nextline := false
        str_nextline := ""
        //log.Printf("msgid='%s' line=%d", msgid, i)
        // get the next line if any
        if nextline < len(header)-1 {
            if header[nextline] != "" {
                str_nextline = header[nextline] // next line as string
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
                ovl.Subject = ovl.Subject+" "+strings.TrimSpace(str_nextline)
            }
        } else
        if !has_from && header_key_L == "from" {
            has_from = true
            ovl.Checksum++
            ovl.From = header_dat
            if spaced_nextline {
                ovl.From = ovl.From+" "+strings.TrimSpace(str_nextline)
            }
        } else
        if !has_date && header_key_L == "date" {
            has_date = true
            ovl.Checksum++
            ovl.Date = header_dat
            /*if spaced_nextline {
                ovl.Date += strings.TrimSpace(str_nextline)
            }*/
        } else
        if !has_msgid && header_key_L == "message-id" {
            if !has_msgid && msgid == "?" && isvalidmsgid(header_dat, false) {
                msgid = header_dat
                ovl.Messageid = msgid
                has_msgid = true
                ovl.Checksum++
            } else
            if !has_msgid && msgid != "?" && msgid == header_dat {
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
        } else
        if !has_refs && header_key_L == "references" {
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

        } else

        if !has_news && header_key_L == "newsgroups" {
            newsgroups_str := header_dat
            if spaced_nextline {
                newsgroups_str += strings.TrimSpace(str_nextline)
            }
            if DEBUG_OV { log.Printf("extract_overview msgid='%s' newsgroups='%s'", msgid, newsgroups_str) }
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
    if DEBUG_OV { log.Printf("Split_References input='%s'", astring) }
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
                if DEBUG_OV { log.Printf("ERROR Split_References !isvalidmsgid='%s' i=%d", aref, i) }
                e++
            }
        }
        if DEBUG_OV { log.Printf("Split_References input='%s' returned=%d e=%d j=%d", astring, len(references), e, j) }

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
        if debug { log.Printf("Removed Nul char from head msgid='%s'", msgid) }
        line = strings.Replace(line, "\x00", " ", -1)
    }

    if strings.Contains(line, "\010") {
        if debug { log.Printf("Removed bkspc char from head msgid='%s'", msgid) }
        line = strings.Replace(line, "\010", " ", -1)
    }

    if strings.Contains(line, "\t") {
        if debug { log.Printf("Removed tab char from head msgid='%s'", msgid) }
        line = strings.Replace(line, "\t", " ", -1)
    }

    if strings.Contains(line, "\n") {
        if debug { log.Printf("Removed newline char from head msgid='%s'", msgid) }
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
    newsgroup = strings.Replace(newsgroup, "!", "", -1) // clear char !
    //newsgroup = strings.Replace(newsgroup, ";", ".", -1) // replace char " with .
    newsgroup = strings.Replace(newsgroup, "\"", ".", -1) // replace char " with .
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

    check_chars := "abcdefghijklmnopqrstuvwxyz0123456789"
    for {
        first_char := string(newsgroup[0])
        if strings.Contains(check_chars, first_char) {
            break
        }
        if len(newsgroup) > 1 {
            newsgroup = newsgroup[1:]
        } else { break }
    }

    if newsgroup == "" {
        return ""
    }

    imat2, stopat2 := 0, 10
    for {
        if newsgroup != "" &&
            ( newsgroup[len(newsgroup)-1] == '.' || // clean some trailing chars
              newsgroup[len(newsgroup)-1] == '/' ||
              newsgroup[len(newsgroup)-1] == '\\' ||
              newsgroup[len(newsgroup)-1] == ',' ||
              newsgroup[len(newsgroup)-1] == ';' ) {
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
        newsgroup = strings.ToLower(newsgroup)
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
    } else
    if !has_commas && has_spaces {
        newsgroups_dirty = strings.Split(newsgroups_str, " ")
    } else
    if !has_commas && !has_spaces  {
        //newsgroups_dirty = strings.Split(newsgroups_str, " ")
        newsgroups_dirty = strings.Split(newsgroups_str, " ")
    } else
    if has_commas && has_spaces {
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
        if o >= LIMIT_SPLITMAX_NEWSGROUPS || (e >= LIMIT_SPLITMAX_NEWSGROUPS && o >= 1) || e > 100 {
            log.Printf("ERROR BREAK Split_NewsGroups msgid='%s' o=%d e=%d len_dirty=%d", msgid, o, e, len_dirty)
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

    for _, r := range group[:0] { // first char of group has to be a letter or digit
        if unicode.IsUpper(r) || r == 0 || !unicode.IsLetter(r) || !unicode.IsDigit(r) {
            log.Printf("!IsValidGroupName #1 group='%s' r=x'%x'", group, string(r))
            return false
        }
    }

    if len(group) > 1 {
        loop_check_chars:
        for i, r := range group[1:] { // check chars without first char, as we it checked before
            valid := false
            // check if this char is not Uppercased and is unicode letter or digit
            if !unicode.IsUpper(r) && (unicode.IsLetter(r) || unicode.IsDigit(r)) {
                valid = true
            } else if !unicode.IsUpper(r) {
                // is not letter or digit, check more chars
                // these are valid for groups
                switch(string(r)) {
                    case ".": valid = true
                    case "_": valid = true
                    case "-": valid = true
                    case "+": valid = true
                    case "&": valid = true
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


func Test_Overview(file_path string, update_footer bool, DEBUG bool) (bool) {
    update_footer = false

    if ovfh, err := Open_ov(file_path); err != nil {
        log.Printf("ERROR OV TEST_Overview Open_ov err='%v' fp='%s'", err, filepath.Base(file_path))
        return false

    } else {
        if err := Close_ov(ovfh, update_footer, true); err != nil {
            log.Printf("ERROR OV TEST_Overview Close_ov err='%v' fp='%s'", err, filepath.Base(file_path))
            return false
        }
    }
    if DEBUG { log.Printf("OK Test_Overview fp='%s'", file_path) }
    return true
} // end func Test_Overview


func get_hash_from_file(file_path string) (string, error){
    file := filepath.Base(file_path)
    if !strings.HasSuffix(file, ".overview") {
        return "", fmt.Errorf("ERROR get_hash_from_file file_path='%s' !HasSuffix .overview", file_path)
    }

    hash := strings.Split(file, ".")[0]
    if len(hash) != 64 {
        return "", fmt.Errorf("ERROR Open_ov len(hash) != 64 fp='%s'", filepath.Base(file_path))
    }
    return hash, nil
}


func Open_ov(file_path string) (OVFH, error) {
    var err error
    var hash string
    if hash, err = get_hash_from_file(file_path); err != nil {
        err = fmt.Errorf("ERROR Open_ov -> hash err='%v' fp='%s'", err, filepath.Base(file_path))
        return OVFH{}, err
    }



    // pass request to handler channel and wait on our supplied reply_channel

    // create open_request
    var open_request Overview_Open_Request
    reply_chan := make(chan Overview_Reply, 1) // FIXME: mark*836b04be* can we not create the reply_chan everytime? just pass it from upper end to here and reuse it?
    open_request.reply_chan = reply_chan
    open_request.file_path = file_path
    open_request.hash = hash

    // pass open_request to open_request_chan
    //globalCounter.Inc("wait_open_request")
    open_request_chan <- open_request

    // wait for reply
    if DEBUG_OV { log.Printf("WAITING Open_ov -> reply_chan fp='%s'", filepath.Base(file_path)) }
    reply := <- reply_chan
    // got reply
    if DEBUG_OV { log.Printf("GOT REPLY Open_ov -> reply_chan fp='%s'", filepath.Base(file_path)) }
    //globalCounter.Dec("wait_open_request")

    if reply.err == nil {

        if DEBUG_OV { log.Printf("OK REPLY Open_ov -> fp='%s'", filepath.Base(file_path)) }
        return reply.ovfh, nil

    } else {
        err = reply.err
        log.Printf("ERROR REPLY Open_ov -> reply_chan err='%v' fp='%s'", err, filepath.Base(file_path))
    }



    return OVFH{}, fmt.Errorf("ERROR Open_ov final err='%v' fp='%s'", err, filepath.Base(file_path))
} // end func Open_ov


func handle_open_ov(hash string, file_path string) (OVFH, error) {
    var err error
    var file_handle *os.File
    var mmap_handle mmap.MMap
    var ov_footer string
    cs := 0

    if !utils.FileExists(file_path) {
        return OVFH{}, fmt.Errorf("ERROR Open_ov !fileExists fp='%s'", file_path)
    }

    if file_handle, err = os.OpenFile(file_path, os.O_RDWR, 0644); err != nil {
        log.Printf("ERROR Open_ov -> mmap.Map err='%v' cs=%d fp='%s'", err, cs, file_path)
        return OVFH{}, err
    }
    cs++ // 1

    if mmap_handle, err = mmap.Map(file_handle, mmap.RDWR, 0); err != nil {
        log.Printf("ERROR Open_ov -> Read_Head_ov err='%v' cs=%d fp='%s'", err, cs, file_path)
        return OVFH{}, err
    }
    cs++ // 2

    //mmap_size := uint64(len(mmap_handle))
    time_open, time_flush, written := utils.Now(), utils.Now(), uint64(0)

    var ovfh OVFH // { file_path, file_handle, mmap_handle, mmap_size, time_open, time_flush, written, 0, 0 }
    ovfh.File_path = file_path
    ovfh.File_handle = file_handle
    ovfh.Mmap_handle = mmap_handle
    ovfh.Mmap_size = uint64(len(ovfh.Mmap_handle))
    ovfh.Mmap_range = ovfh.Mmap_size-1
    ovfh.Time_open = time_open
    ovfh.Time_flush = time_flush
    ovfh.Written = written
    ovfh.Hash = hash


    if _, err := Read_Head_ov(ovfh); err != nil { // _ = ov_header
        log.Printf("ERROR Open_ov -> Read_Head_ov err='%v' cs=%d fp='%s'", err, cs, file_path)
        return OVFH{}, err
    }
    cs++ // 3

    if ov_footer, err = Read_Foot_ov(ovfh); err != nil {
        log.Printf("ERROR Open_ov -> Read_Foot_ov err='%v' cs=%d fp='%s'", err, cs, file_path)
        return OVFH{}, err
    }
    cs++ // 4

    foot := strings.Split(ov_footer, ",")
    if len(foot) != SIZEOF_FOOT {
        log.Printf("ERROR Open_ov -> len(foot)=%d != SIZEOF_FOOT=%d fp='%s'", len(foot), SIZEOF_FOOT, file_path)
        return OVFH{}, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
    }
    cs++ // 5

    if !strings.HasPrefix(foot[1], "last=") ||
        !strings.HasPrefix(foot[2], "Findex=") ||
        !strings.HasPrefix(foot[3], "bodyend=") ||
        !strings.HasPrefix(foot[4], "fend=") {
        log.Printf("ERROR Open_ov -> error !HasPrefix foot fp='%s'", file_path)
        return OVFH{}, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
    }
    cs++ // 6

    last, findex := utils.Str2uint64(strings.Split(foot[1], "=")[1]), utils.Str2uint64(strings.Split(foot[2], "=")[1])
    bodyend, fend := utils.Str2uint64(strings.Split(foot[3], "=")[1]), utils.Str2uint64(strings.Split(foot[4], "=")[1])
    if findex >= bodyend && bodyend != fend-uint64(OV_RESERVE_END) {
        log.Printf("ERROR Open_ov -> findex=%d > bodyend=%d ? fend=%d fp='%s'", findex, bodyend, fend, file_path)
        return OVFH{}, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
    }
    cs++ // 7

    if findex < OV_RESERVE_BEG || findex > OV_RESERVE_BEG && last == 0 {
        log.Printf("ERROR Open_ov -> findex=%d OV_RESERVE_BEG=%d last=%d fp='%s'", findex, OV_RESERVE_BEG, last, file_path)
        return OVFH{}, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
    }
    cs++ // 8


    ovfh.Findex = findex
    ovfh.Last = last
    if retbool, ovfh := Replay_Footer(ovfh); retbool == true {
        return ovfh, nil
    } else {
        log.Printf("ERROR Open_ov -> Replay_Footer fp='%s' retbool=%t", file_path, retbool)

    }
    return OVFH{}, fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)

    /*
    if file_handle, err := os.OpenFile(file_path, os.O_RDWR, 0644); err == nil {
        cs++ // 1
        if mmap_handle, err := mmap.Map(file_handle, mmap.RDWR, 0); err == nil {
            cs++ // 2
            mmap_size := len(mmap_handle)
            time_open, time_flush, written := Now(), Now(), uint64(0)
            //Findex := 0 // FIX_ME: read last Findex from footer
            ovfh := OVFH{ file_path, file_handle, mmap_handle, mmap_size, time_open, time_flush, written, 0, 0 }
            if _, err := Read_Head_ov(ovfh); err == nil { // _ = ov_header
                cs++ // 3
                if ov_footer, err := Read_Foot_ov(ovfh); err == nil {
                    cs++ // 4
                    foot := strings.Split(ov_footer, ",")
                    if len(foot) == SIZEOF_FOOT {
                        cs++ // 5
                        if strings.HasPrefix(foot[1], "last=") &&
                            strings.HasPrefix(foot[2], "Findex=") &&
                            strings.HasPrefix(foot[3], "bodyend=") &&
                            strings.HasPrefix(foot[4], "fend=") {
                            cs++ // 6
                            last, findex := utils.Str2int(strings.Split(foot[1], "=")[1]), utils.Str2int(strings.Split(foot[2], "=")[1])
                            bodyend, fend := utils.Str2int(strings.Split(foot[3], "=")[1]), utils.Str2int(strings.Split(foot[4], "=")[1])
                            if findex < bodyend && bodyend == fend-OV_RESERVE_END {
                                cs++ // 7
                                //last, findex := utils.Str2int(last_str), utils.Str2int(findex_str)
                                if findex >= OV_RESERVE_BEG && last >= 0 {
                                    cs++ // 8
                                    ovfh.Findex = findex
                                    ovfh.Last = last
                                    if retbool, ovfh := Replay_Footer(ovfh); retbool == true {
                                        return ovfh, nil
                                    } else {
                                        log.Printf("ERROR Open_ov -> Replay_Footer fp='%s' retbool=%t", file_path, retbool)
                                        reterr = fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
                                    }

                                } else {
                                    log.Printf("ERROR Open_ov -> findex=%d < OV_RESERVE_BEG=%d fp='%s'", findex, OV_RESERVE_BEG, file_path)
                                    reterr = fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
                                }

                            } else {
                                log.Printf("ERROR Open_ov -> findex=%d > bodyend=%d ? fend=%d fp='%s'", findex, bodyend, fend, file_path)
                                reterr = fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
                            }

                        } else {
                            log.Printf("ERROR Open_ov -> error HasPrefix foot fp='%s'", file_path)
                            reterr = fmt.Errorf("ERROR Open_ov cs=%d fp='%s'", cs, file_path)
                        }

                    } else {
                        log.Printf("ERROR Open_ov -> len(foot)=%d != SIZEOF_FOOT=%d fp='%s'", len(foot), SIZEOF_FOOT, file_path)
                        reterr =
                    }

                } else {
                    log.Printf("ERROR Open_ov -> Read_Foot_ov err='%v' fp='%s'", file_path, err)
                    reterr = err
                }

            } else {
                log.Printf("ERROR Open_ov -> Read_Head_ov err='%v' fp='%s'", file_path, err)
                reterr = err
            }

        } else {
            log.Printf("ERROR Open_ov -> Read_Head_ov err='%v' fp='%s'", file_path, err)
            reterr = err
        }

    } else {
        log.Printf("ERROR Open_ov -> mmap.Map err='%v' fp='%s'", file_path, err)
        reterr = err
    }

    log.Printf("ERROR Open_ov cs=%d reterr='%v'", cs, reterr)
    return OVFH{}, reterr
    */
} // end func Open_overview


func Close_ov(ovfh OVFH, update_footer bool, force_close bool) error {
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
    if DEBUG_OV { log.Printf("WAITING Close_ov -> reply_chan fp='%s'", file) }
    reply := <- reply_chan
    // got reply
    if DEBUG_OV { log.Printf("GOT REPLY Close_ov -> reply_chan fp='%s'", file) }
    //globalCounter.Dec("wait_close_request")
    if reply.err == nil {
        if DEBUG_OV { log.Printf("OK REPLY Close_ov -> fp='%s'", file) }
    } else {
        err = reply.err
        log.Printf("ERROR REPLY Close_ov -> reply_chan err='%v' fp='%s'", err, file)
    }

    return err
} // end func Close_ov


func handle_close_ov(ovfh OVFH, update_footer bool, force_close bool, grow bool) error {
    var err error
    if update_footer {
        if ovfh, err = Update_Footer(ovfh); err != nil {
            return err
        }
    }

    if err = ovfh.Mmap_handle.Flush(); err == nil {
        if err = ovfh.Mmap_handle.Unmap(); err == nil {
            if err = ovfh.File_handle.Close(); err == nil {
                if DEBUG_OV { log.Printf("Close_ov update_footer=%t grow=%t OK fp='%s'", update_footer, grow, ovfh.File_path) }
                return nil
            }
        }
    }
    log.Printf("ERROR Close_ov fp='%s' err='%v'", ovfh.File_path, err)
    return err
} // end func close_overview


func Flush_ov(ovfh OVFH) (error) {
    var err error
    if err = ovfh.Mmap_handle.Flush(); err != nil {
        log.Printf("ERROR Flush_ov fp='%s' err='%v'", ovfh.File_path, err)
    } else {
        if DEBUG_OV { log.Printf("Flush_ov OK fp='%s'", ovfh.File_path) }
    }
    return err
} // end func flush_overview


func Replay_Footer(ovfh OVFH) (bool, OVFH) {

    if ovfh.Last == 0 && ovfh.Findex == uint64(OV_RESERVE_BEG) {
        if DEBUG_OV { log.Printf("Replay_Footer NEW OK ovfh.Findex=%d", ovfh.Findex) }
        return true, ovfh
    }
    position := uint64(1)
    needs_trues := 2
    startindex, endindex := ovfh.Findex, uint64(0)
    newlines, tabs, frees, trues := 0, 0 , uint64(0), 0

    replay:
    for {
        c := ovfh.Mmap_handle[startindex] // check char at index
        if DEBUG_OV { log.Printf("DEBUG REPLAY: c='%x' startindex=%d", c, startindex) }

        switch(c) {
            case 0:
                // char is a <nul> and has to be at 1st position
                if position == 1 {
                    //trues++
                } else {
                    log.Printf("ERROR OV c=nul position=%d startindex=%d endindex=%d Findex=%d fp='%s'", position, startindex, endindex, ovfh.Findex, ovfh.File_path)
                }
                frees++  // should only be 1 free byte, always
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
                switch(newlines) {
                    case 1:
                        // found first newline
                        if position == 2 || position - frees == 1 {
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

        if startindex < uint64(OV_RESERVE_BEG) {
            // dont run into the header...
            log.Printf("ERROR Replay Footer startindex=%d < OV_RESERVE_BEG=%d fp='%s'", startindex, OV_RESERVE_BEG, ovfh.File_path)
            break
        }
        startindex--    // decrease index to walk backwards
        position++      // count position upwards

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

    if DEBUG_OV { log.Printf("OK Replay Footer %s", retstring) }

    //log.Printf("Replay_Footer PRE tabs=%d newlines=%d frees=%d", tabs, newlines, frees)

    // read the last line and get the first field with last written article number to file,
    // which should be the ovh.Last value - 1
    lastline := string(ovfh.Mmap_handle[startindex+1:endindex])
    if DEBUG_OV { log.Printf("Replay_Footer lastline='%s'", lastline) }
    last_str := strings.Split(lastline, "\t")[0]
    if frees > 1 {
        oldFindex := ovfh.Findex
        diff := frees-1
        ovfh.Findex -= diff
        log.Printf("WARN OV adjusted frees=%d oldFindex=%d - diff=%d = Findex=%d", frees, oldFindex, diff, ovfh.Findex)
        time.Sleep(5 * time.Second) // DEBUG SLEEP

    }
    if utils.IsDigit(last_str) {
        if utils.Str2uint64(last_str) == ovfh.Last-1 {
            if DEBUG_OV { log.Printf("Replay_Footer OK fp='%s' last=%s next=%d", ovfh.File_path, last_str, ovfh.Last) }
            return true, ovfh
        }
    }
    log.Printf("ERROR Replay_Footer last=%s ovfh.Last=%d  fp='%s' ", last_str, ovfh.Last, ovfh.File_path)
    return false, ovfh
} // end Replay_Footer


func Write_ov(ovfh OVFH, data string, is_head bool, is_foot bool, grow bool) (OVFH, error, string) {
    var err error
    //len_data := len(data)
    databyte := []byte(data)
    len_data := uint64(len(databyte))
    mmap_size := uint64(len(ovfh.Mmap_handle))
    //mmap_range := uint64(mmap_size-1)
    if DEBUG_OV { log.Printf("Write_ov len_data=%d is_head=%t is_foot=%t", len_data, is_head, is_foot) }

    // set start Findex vs reserved space at beginning of map
    if !is_head && !is_foot && ovfh.Findex < uint64(OV_RESERVE_BEG) {
        ovfh.Findex = uint64(OV_RESERVE_BEG)
    }

    //
    if mmap_size != ovfh.Mmap_size {
        return OVFH{}, fmt.Errorf("ERROR Write_ov len(ovfh.Mmap_handle) != ovfh.Mmap_size=%d fp='%s'", mmap_size, ovfh.Mmap_size, ovfh.File_path), ""
    }

    if ovfh.Mmap_range < OV_RESERVE_BEG + OV_RESERVE_END {
        return OVFH{}, fmt.Errorf("ERROR Write_ov ovfh.Mmap_size=%d fp='%s'", ovfh.Mmap_size, ovfh.File_path), ""
    }

    bodyspace := ovfh.Mmap_size - OV_RESERVE_BEG - OV_RESERVE_END
    if bodyspace <= 0 {
        return ovfh, fmt.Errorf("ERROR Write_ov bodyspace=%d fp='%s'", bodyspace, ovfh.File_path), ""
    }

    // dont count OV_RESERVE_BEG here as it is already included in Findex
    bodyend := ovfh.Mmap_range - OV_RESERVE_END
    freespace := bodyend - ovfh.Findex
    //freespace := ovfh.Mmap_range - ovfh.Findex - OV_RESERVE_END
    // newsize adds len of data to Findex
    // then newsize should not be higher than freespace
    newbodysize := len_data + ovfh.Findex

    if DEBUG_OV {
        log.Printf("Write_ov bodyspace=%d len_data=%d freespace=%d Findex=%d newsize=%d",len_data , bodyspace, freespace, ovfh.Findex, newbodysize)
    }

    if !is_foot && (freespace <= 1 || newbodysize >= bodyend) {
        if DEBUG_OV {
            log.Printf("GROW OVERVIEW Findex=%d len_data=%d freespace=%d bodyend=%d newsize=%d hash='%s'", ovfh.Findex, len_data , freespace, bodyend, newbodysize, ovfh.Hash)
        }

        if ovfh, err = Grow_ov(ovfh, 1, "128K", 0); err != nil {
            overflow_err := fmt.Errorf("ERROR Write_ovfh -> Grow_ov err='%v' newsize=%d avail=%d mmap_size=%d", err, newbodysize, freespace, ovfh.Mmap_size)
            return ovfh, overflow_err, ERR_OV_OVERFLOW
        }
        if DEBUG_OV { log.Printf("DONE GROW OVERVIEW hash='%s'", ovfh.Hash) }
    }

    if is_foot && data == "" && grow == true {
        // zerofill-overwrite the footer space
        index := ovfh.Mmap_size - OV_RESERVE_END
        databyte := []byte(zerofill(ZERO_PATTERN, int(OV_RESERVE_END)))
        // writes data to mmap byte for byte
        for pos, abyte := range databyte {
            if index >= ovfh.Mmap_size {
                if DEBUG_OV { log.Printf("Write_ov GROW fp='%s' reached end index=%d mmap_size=%d pos=%d len_databyte=%d break", ovfh.File_path, index, ovfh.Mmap_size, pos+1, len(databyte)) }
                break
            }
            ovfh.Mmap_handle[index] = abyte
            index++

        }

    } else

    if is_foot && data != "" { // footer data is not empty, write footer
        startindex := ovfh.Mmap_size - OV_RESERVE_END
        // writes data to mmap byte for byte
        for _, abyte := range databyte {
            ovfh.Mmap_handle[startindex] = abyte
            startindex++
        }
        ovfh.Written += uint64(len(databyte))

    } else
    if !is_head && !is_foot && data != "" { // write data

        startindex := ovfh.Findex
        limit := ovfh.Mmap_range - OV_RESERVE_END
        if ovfh.Mmap_handle == nil {
            return OVFH{}, fmt.Errorf("ERROR overview.Write_ovfh Mmap_handle == nil fp='%s'", ovfh.File_path), ""
        }
        if DEBUG_OV { log.Printf("Write_ov data=%d Findex=%d limit=%d range=%d handle=%d fp='%s'", len(data), startindex, limit, ovfh.Mmap_range, len(ovfh.Mmap_handle), ovfh.File_path) }

        /*
        for {
            if startindex >= limit {
                overflow_err := fmt.Errorf("ERROR overview.Write_ovfh overflow startindex=%d > limit=%d len_data=%d", startindex, ovfh.Mmap_size, limit, len_data)
                return ovfh, overflow_err, ERR_OV_OVERFLOW
            }
            if ovfh.Mmap_handle[startindex] == '\x00' { // 0 is a nul byte
                if startindex != ovfh.Findex {
                    log.Printf("Write_ov fp='%s' bad Findex old=%d new=%d", ovfh.File_path, ovfh.Findex, startindex)
                    ovfh.Findex = startindex
                }
                break
            }
            // Findex is not zeroed?? can not write here... find next continues free space
            startindex++

        } // end for
        */

        // writes data to mmap byte for byte
        for _, abyte := range databyte {
            if ovfh.Findex >= limit {
                break
            }
            ovfh.Mmap_handle[ovfh.Findex] = abyte
            ovfh.Findex++
        }
        ovfh.Written += uint64(len(databyte))

    } // !is_head && ! is_foot
    return ovfh, nil, ""
} // end func write_overview


func Read_Head_ov(ovfh OVFH) (string, error) {
    //mmap_size := len(ovfh.Mmap_handle)
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


func Read_Foot_ov(ovfh OVFH) (string, error) {
    //mmap_size := len(ovfh.Mmap_handle)
    foot_start := ovfh.Mmap_size - OV_RESERVE_END
    if foot_start > OV_RESERVE_END {
        ov_footer := string(ovfh.Mmap_handle[foot_start:])
        if check_ovfh_footer(ov_footer) {
            if DEBUG_OV { log.Printf("OK Read_Foot_ov -> check_ovfh_footer") }
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
    bytesize := 1024
    if pages >= 1 {
        bytesize = pages*1*1024
    }
    now := utils.Now()
    //file_name := filepath.Base(File_path)
    //hash := hash256(group)
    head_str := fmt.Sprintf("%s%d,group=%s,zeropad=%s,%s", HEADER_BEG, now, hash, ZERO_PATTERN, HEADER_END)
    bodyend := OV_RESERVE_BEG+uint64(bytesize)
    foot_str := fmt.Sprintf("%s%d,last=0,Findex=%d,bodyend=%d,fend=%d,zeropad=%s,%s", FOOTER_BEG, now, OV_RESERVE_BEG, bodyend, bodyend+OV_RESERVE_END, ZERO_PATTERN, FOOTER_END)
    ov_header := zerofill(head_str, int(OV_RESERVE_BEG))
    ov_body := zerofill_block(pages, "1K") // initial max of overview data, will grow when needed
    ov_footer := zerofill(foot_str, int(OV_RESERVE_END))

    wb, wbt := 0, 0
    if wb, err = init_file(File_path, ov_header, false); err == nil {
        wbt += wb
        if wb, err = init_file(File_path, ov_body, false); err == nil {
            wbt += wb
            //ov_footer := construct_footer(ovfh, wbt)
            if wb, err = init_file(File_path, ov_footer, false); err == nil {
                wbt += wb
                // WORK_HERE
                if DEBUG_OV { log.Printf("Create_ov OK fp='%s' wbt=%d", File_path, wbt) }
                return nil
            }
        }
    }
    log.Printf("ERROR Create_ov fp='%s' wbt=%d", File_path, wbt)
    return err
} // end func Create_ov


func Grow_ov(ovfh OVFH, pages int, blocksize string, mode int) (OVFH, error) {
    var err error
    var errstr string
    var header string
    var footer string
    var wbt int
    if !utils.FileExists(ovfh.File_path) {
        return ovfh, fmt.Errorf("ERROR Grow_ov !exists ovfh.fp='%s'", ovfh.File_path)
    }
    if DEBUG_OV { log.Printf("Grow_ov pages=%d bs=%s fp='%s'", pages, blocksize, ovfh.File_path) }

    if mode != 999 { // dont do these checks if we want to fix overview footer

        // update footer
        if ovfh, err := Update_Footer(ovfh); err != nil {
            return ovfh, err
        }

        // check header
        if header, err = Read_Head_ov(ovfh); err != nil {
            return ovfh, err
        }
        if retbool := check_ovfh_header(header); !retbool {
            return ovfh, fmt.Errorf("ERROR Grow_ov -> check_ovfh_header fp='%s' header='%s' retbool=false", ovfh.File_path, header)
        }
        if DEBUG_OV { log.Printf("Grow_ov fp='%s' check_ovfh_header OK Findex=%d", ovfh.File_path, ovfh.Findex) }

        // check footer
        if footer, err = Read_Foot_ov(ovfh); err != nil {
            return ovfh, err
        }
        if retbool := check_ovfh_footer(footer); !retbool {
            return ovfh, fmt.Errorf("ERROR Grow_ov -> check_ovfh_footer fp='%s' footer='%s' retbool=false", ovfh.File_path, footer)
        }
        if DEBUG_OV { log.Printf("Grow_ov fp='%s' check_ovfh_footer OK Findex=%d", ovfh.File_path, ovfh.Findex) }


        // 1. overwrite footer area while still mapped
        if ovfh, err, errstr = Write_ov(ovfh, "", false, true, true); err != nil {
            log.Printf("ERROR Grow_ov -> Write_ov err='%v' errstr='%s'", err, errstr)
            return ovfh, err
        }

    } // end if mode != 999

    // 2. unmap and close overview mmap
    //force_close := true
    //if err = Close_ov(ovfh, false, force_close); err != nil {
    if err = handle_close_ov(ovfh, false, false, true); err != nil {
        return ovfh, err
    }
    if DEBUG_OV { log.Printf("Grow_ov fp='%s' mmap closed OK", ovfh.File_path) }


    // 3. extend the overview body
    if wb, err := init_file(ovfh.File_path, zerofill_block(pages, blocksize), true); err != nil {
        log.Printf("ERROR Grow_ov -> init_file err='%v'", err)
        return ovfh, err
    } else {
        wbt += wb
    }
    if DEBUG_OV { log.Printf("Grow_ov fp='%s' zerofill_block=%d OK", ovfh.File_path, wbt) }


    // 4. append footer
    ov_footer := construct_footer(ovfh)
    if wb, err := init_file(ovfh.File_path, ov_footer, true); err != nil {
        log.Printf("ERROR Grow_ov -> init_file2 err='%v'", err)
        return ovfh, err
    } else {
        wbt += wb
    }
    // footer appended


    // 5. reopen mmap file
    //if ovfh, err = Open_ov(ovfh.File_path); err != nil {
    if ovfh, err = handle_open_ov(ovfh.Hash, ovfh.File_path); err != nil {
        log.Printf("ERROR Grow_ov -> Open_ov err='%v'", err)
        return ovfh, err
    }

    // 6. done
    body_end := ovfh.Mmap_size - OV_RESERVE_END
    if DEBUG_OV { log.Printf("Grow_ov OK fp='%s' wbt=%d body_end=%d Findex=%d", ovfh.File_path, wbt, body_end, ovfh.Findex) }
    return ovfh, nil


    /* REMOVE OLD !!! */
    /*
    if ovfh, err = Update_Footer(ovfh); err == nil {

        if footer, err := Read_Foot_ov(ovfh); err == nil {

            if retbool := check_ovfh_footer(footer); !retbool {
                return ovfh, fmt.Errorf("ERROR Grow_ov -> check_ovfh_footer fp='%s' footer='%s' retbool=false", ovfh.File_path, footer)
            }
            if DEBUG_OV { log.Printf("Grow_ov fp='%s' check_ovfh_footer OK Findex=%d", ovfh.File_path, ovfh.Findex) }

            // FIXME IDENTICAL COPY IN Rescan_Overview !
            // 1. remove footer while file is still mapped
            ovfh, err, errstr = Write_ov(ovfh, "", false, true, true);
            if err == nil {
                // WORK_HERE
                // close the mmap file and append nuls via ioutils
                if err = Close_ov(ovfh, false); err == nil {
                    if DEBUG_OV { log.Printf("Grow_ov fp='%s' mmap closed OK", ovfh.File_path) }
                    // 2. extend the overview body
                    if wb, err := init_file(ovfh.File_path, zerofill_block(pages, blocksize), true); err == nil {
                        wbt += wb
                        if DEBUG_OV { log.Printf("Grow_ov fp='%s' zerofill_block=%d OK", ovfh.File_path, wb) }
                        // 3. append new footer

                        //new_foot_str := fmt.Sprintf("%s%d,last=%d,Findex=%d,bodyend=%d,zeropad=%s,%s", FOOTER_BEG, Now(), ovfh.Last, ovfh.Findex, body_end, ZERO_PATTERN, FOOTER_END)
                        //ov_footer := zerofill(new_foot_str, OV_RESERVE_END)
                        ov_footer := construct_footer(ovfh)
                        if wb, err = init_file(ovfh.File_path, ov_footer, true); err == nil {
                            wbt += wb
                            // footer appended

                            // reopen mmap file
                            ovfh, err = Open_ov(ovfh.File_path)
                            if err != nil {
                                log.Fatalf("ERROR Grow_ov -> Open_ov err='%v'", err)
                            }
                            body_end := ovfh.Mmap_size - OV_RESERVE_END
                            if DEBUG_OV { log.Printf("Grow_ov OK fp='%s' wbt=%d body_end=%d Findex=%d", ovfh.File_path, wbt, body_end, ovfh.Findex) }
                            return ovfh, nil
                        }
                    }
                }
            } else {
                log.Printf("ERROR Grow_ov -> Write_ov err='%v' errstr='%s'", err, errstr)
            }

        }
    }
    return ovfh, err
    */
} // end func Grow_ov


func Update_Footer(ovfh OVFH) (OVFH, error) {
    if ovfh.Findex == 0 || ovfh.Last == 0 {
        return OVFH{}, fmt.Errorf("ERROR Update_Footer ovfh.Findex=%d ovfh.Last=%d", ovfh.Findex, ovfh.Last)
    }
    var err error
    //var errstr string
    ov_footer := construct_footer(ovfh)
    ovfh, err, _ = Write_ov(ovfh, ov_footer, false, true, false);
    if err != nil {
        log.Printf("ERROR Update_Footer -> Write_ov err='%v'", err)
    } else {
        if DEBUG_OV { log.Printf("OK Update_Footer -> Write_ov len_ov_footer=%d", len(ov_footer)) }
    }
    return ovfh, err
} // end Update_Footer


// private overview functions

func construct_footer(ovfh OVFH) string {
    bodyend := ovfh.Mmap_size - OV_RESERVE_END
    foot_str := fmt.Sprintf("%s%d,last=%d,Findex=%d,bodyend=%d,fend=%d,zeropad=%s,%s", FOOTER_BEG, utils.Nano(), ovfh.Last, ovfh.Findex, bodyend, bodyend+OV_RESERVE_END, ZERO_PATTERN, FOOTER_END)
    ov_footer := zerofill(foot_str, int(OV_RESERVE_END))
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
} // end func check_header


func check_ovfh_footer(footer string) (bool) {
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
} // end func check_footer


func init_file(File_path string, data string, grow bool) (int, error) {
    if DEBUG_OV { log.Printf("init_file fp='%s' len_data=%d grow=%t", File_path, len(data), grow) }
    var fh *os.File
    var err error
    var wb int
    if fh, err = os.OpenFile(File_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
        defer fh.Close()
        w := bufio.NewWriter(fh)
        if wb, err = w.WriteString(data); err == nil {
            if err = w.Flush(); err == nil {
                if DEBUG_OV { log.Printf("init_file wrote fp='%s' len_data=%d wb=%d grow=%t", File_path, len(data), wb, grow) }
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
        log.Printf("PRELOAD_ZERO_1K=%d", len(PRELOAD_ZERO_1K))
    } else
    if what == "PRELOAD_ZERO_4K" && PRELOAD_ZERO_4K == "" {
        PRELOAD_ZERO_4K = zerofill_block(4, "1K")
        log.Printf("PRELOAD_ZERO_4K=%d", len(PRELOAD_ZERO_4K))
    } else
    if what == "PRELOAD_ZERO_128K" && PRELOAD_ZERO_128K == "" {
        PRELOAD_ZERO_128K = zerofill_block(32, "4K")
        log.Printf("PRELOAD_ZERO_128K=%d", len(PRELOAD_ZERO_128K))
    }/* else
    if what == "PRELOAD_ZERO_1M" && PRELOAD_ZERO_1M == "" {
        PRELOAD_ZERO_1M = zerofill_block(8, "128K")
    }*/
} // end func preload_zero


func zerofill(astring string, fillto int) string {
    //log.Printf("zerofill len_astr=%d fillto=%d", len(astring), fillto)
    strlen := len(astring)-ZERO_PATTERN_LEN
    diff := fillto - strlen
    zf := ""
    for i := 0; i < diff; i++ {
        zf += ZERO_FILL_STR
    }
    retstring := strings.Replace(astring, ZERO_PATTERN, zf, 1)
    if DEBUG_OV { log.Printf("zerofill astring='%s' input=%d diff=%d output=%d zf=%d", astring, strlen, diff, len(retstring), len(zf)) }
    return retstring
} // end func zerofill


func zerofill_block(pages int, blocksize string) string {
    if DEBUG_OV { log.Printf("zerofill_block pages=%d blocksize=%s", pages, blocksize) }
    filler := ""
    switch(blocksize) {
        case "1K":
            //preload_zero("PRELOAD_ZERO_1K")
            filler = PRELOAD_ZERO_1K
        case "4K":
            //preload_zero("PRELOAD_ZERO_4K")
            filler = PRELOAD_ZERO_4K
        case "128K":
            //preload_zero("PRELOAD_ZERO_128K")
            filler = PRELOAD_ZERO_128K
        case "1M":
            //preload_zero("PRELOAD_ZERO_1M")
            filler = PRELOAD_ZERO_1M
    }
    zf := ""
    for i := 0; i < pages; i++ {
        zf += filler
    }
    return zf
} // end func zerofill

func is_closed_server(stop_server_chan chan bool) (bool) {
    isclosed := false

    // try reading a true from channel
    select {
        case has_closed, ok := <- stop_server_chan:
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
        if strings.HasPrefix(astring, "<") && strings.HasSuffix(astring, ">") {
            //log.Printf("isvalidmsgid(%s)==true", astring)
            return true
        }
    } else {
        //log.Printf("msgid is digit and valid", astring)
        return true
    }
    if !silent || DEBUG_OV { log.Printf("ERROR !overview.isvalidmsgid('%s')", astring) }
    return false
} // end func isvalidmsgid



// every worker checks vs Open_MMAP_Overviews if any other worker is processing this group right now
// if map[group]bool returns true, place a channel in ch and wait for return signal
type Open_MMAP_Overviews struct {
    v   map[string]int64
    ch  map[string][]chan struct{}
    mux sync.Mutex
}


func (c *Open_MMAP_Overviews) isopenMMAP(hash string) bool {
    retval := false
    c.mux.Lock()
    if c.v[hash] > 0 {
        retval = true
    }
    c.mux.Unlock()
    if DEBUG_OV { log.Printf("isopenMMAP hash='%s' retval=%t", hash, retval) }
    return retval
} // end func isopenMMAP

func (c *Open_MMAP_Overviews) closelockMMAP(hid int, hash string) (bool) {
    retval := false
    c.mux.Lock()
    locktime := c.v[hash]
    if locktime == 0 {
        c.v[hash] = utils.Nano()
        retval = true
    }
    c.mux.Unlock()
    return retval
} // end func closelockMMAP

func (c *Open_MMAP_Overviews) lockMMAP(hid int, hash string) (bool, chan struct{}) {
    var signal_chan chan struct{} = nil
    retval := false

    c.mux.Lock()

    len_cv := len(c.v)
    locktime := c.v[hash]
    if locktime == 0 {
        locktime = utils.Nano()
        if DEBUG_OV { log.Printf("#### OV_H %d) lockMMAP true locktime=%d len_cv=%d hash='%s'", hid, locktime, len_cv, hash) }
        //time.Sleep(1 * time.Second)
        c.v[hash] = locktime
        retval = true
    } else {
        //time.Sleep(3 * time.Second)
        signal_chan = make(chan struct{}, 1)
        c.ch[hash] = append(c.ch[hash], signal_chan)
        if DEBUG_OV { log.Printf("!!!! OV_H %d) islocked locktime=%d -> created signal_chan @pos=%d hash='%s'", hid, locktime, len(c.ch[hash]), hash) }
    }
    c.mux.Unlock()

    if DEBUG_OV { log.Printf("____ OV_H %d) lockMMAP hash='%s' retval=%t ch='%v'", hid, hash, retval, signal_chan) }
    return retval, signal_chan
} // end func lockMMAP


func (c *Open_MMAP_Overviews) unlockMMAP(hid int, force_close bool, hash string) {
    //if DEBUG_OV { log.Printf("<--- OV_H %d) unlockMMAP hash='%s'", hid, hash) }
    var signal_chan chan struct{} = nil
    c.mux.Lock()
    locktime := c.v[hash]
    waiting_worker := len(c.ch[hash])
    if locktime == 0 {
        c.mux.Unlock()
        /*
        if !force_close {
            log.Printf("¡¡¡¡ (CLOSER) OV_H %d) ERROR unlockMMAP: NO locktime force_close=false hash='%s' waiting_worker=%d", hid, hash, waiting_worker)
        }*/
        return
    }

    if waiting_worker > 0 {
        if DEBUG_OV { log.Printf("~~~~ (CLOSER) OV_H %d) unlockMMAP waiting_worker=%d locktime=%d hash='%s' ", hid, waiting_worker, locktime, hash) }
        signal_chan, c.ch[hash] = c.ch[hash][0], c.ch[hash][1:] // popList
    } else {
        if DEBUG_OV { log.Printf("~~~~ (CLOSER) OV_H %d) unlockMMAP waiting_worker=0 clear map locktime=%d hash='%s' ", hid, locktime, hash) }
        delete(c.v, hash)
    }

    c.mux.Unlock()
    if signal_chan != nil {
        if DEBUG_OV { log.Printf("<--- (CLOSER) OV_H %d) unlockMMAP signal locktime=%d  hash='%s'", hid, locktime, hash) }
        signal_chan <- struct{}{}
        close(signal_chan)
    }
} // end func unlockMMAP



