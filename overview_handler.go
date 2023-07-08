package overview


import (
    "fmt"
    "path/filepath"
    "log"
    "sync"
    "time"
    "github.com/go-while/go-utils"
)


type OV_Handler_data struct {
    ovfh            OVFH     // overview mmap file handle struct
    hid             int      // overview mmap is assigned to handler id
    preopen         bool     // overview mmap is opening
    open            bool     // overview mmap is open
    idle            int64    // overview mmap is idle since unix timestamp
}


type OV_Handler struct {
    V   map[string]OV_Handler_data
    Debug bool
    mux sync.Mutex
    STOP chan bool
    MAX_OPEN_MMAPS int
}


func (c *OV_Handler) GetOpen(hid int, file_path string, hash string) (OVFH, bool) {

    // FIXME CODE DUPLICATE *a7fa51ca6*
    if retbool, ch := open_mmap_overviews.lockMMAP(hid, hash); retbool == false && ch != nil {
        if c.Debug { log.Printf("!!!! OV_H %d) GetOpen wait islocked hash='%s'", hid, hash) }
        <- ch // wait for anyone to return the unlock for this group
        if c.Debug { log.Printf("---> OV_H %d) GetOpen got signal unlock hash='%s'", hid, hash) }
    } else
    if retbool == false && ch == nil {
        //c.mux.Unlock()
        // could not lock and didnt return a signal channel? should never happen he says!
        log.Printf("WARN OV_H %d) GetOpen lockMMAP retry hash='%s'", hid , hash)
        time.Sleep(1 * time.Millisecond)
        return OV_handler.GetOpen(hid, file_path, hash)
    } else {
        if c.Debug { log.Printf("TRUE OV_H %d) GetOpen -> lockMMAP retbool=%t ch='%v' hash='%s'", hid , retbool, ch, hash) }
    }


    c.mux.Lock()
    mapdata := c.V[hash]

    if mapdata.hid < 0 {
        c.mux.Unlock()
        if c.Debug { log.Printf("$$$$ OV_H %d) GetOpen retry isin flush/force_close? hid=%d hash='%s'", hid, mapdata.hid, hash) }
        time.Sleep(1 * time.Millisecond)
        return OV_handler.GetOpen(hid, file_path, hash)
    }

    if mapdata.hid > 0 && mapdata.hid != hid {
        c.mux.Unlock()
        if c.Debug { log.Printf("^^^^ OV_H %d) GetOpen retry isassigned hid=%d hash='%s'", hid, mapdata.hid, hash) }
        time.Sleep(1 * time.Millisecond)
        return OV_handler.GetOpen(hid, file_path, hash)
        //return OVFH{}, false // FIXME return value is bad here, need something else, cant open hid -1 or -2 !
    }


    if !utils.FileExists(file_path) {
        if err := Create_ov(file_path, hash, 3); err != nil {
            c.mux.Unlock()
            log.Printf("FATAL ERROR OV_H %d) Create_ov hash='%s' err='%v'", hid, hash, err)
            OV_handler.KILL(hid)
            return OVFH{}, false
        }
        if c.Debug { log.Printf("OV_H %d) Create_ov hash='%s' OK", hid, hash) }
    }

    if c.Debug { log.Printf("OV_H %d) OK GetOpen after lockMMAP hash='%s'", hid, hash) }

    // was not assigned, assign now to us
    mapdata.hid = hid

    if mapdata.open && !mapdata.preopen {
        ovfh := mapdata.ovfh
        c.V[hash] = mapdata // updates the hid
        c.mux.Unlock()
        if c.Debug { log.Printf("OV_H %d) GetOpen returned mapdata.open=true hash='%s'", hid, hash) }
        return ovfh, true
    }

    mapdata.preopen = true
    c.V[hash] = mapdata
    c.mux.Unlock()


    if ovfh, err := handle_open_ov(hash, file_path); err != nil {
        log.Printf("ERROR (OPENER) OV_H %d) Open_ov err='%v' fp='%s'", hid, err, filepath.Base(file_path))
        //reply.err = err
        //open_errors++
    } else {

        if c.Debug { log.Printf("OV_H %d) GetOpen before count_open_overviews=%d/%d", hid, len(count_open_overviews), cap(count_open_overviews)) }
        count_open_overviews <- struct{}{} // pass an empty struct into the open_file counter channel
        if c.Debug { log.Printf("OV_H %d) GetOpen afters count_open_overviews=%d/%d", hid, len(count_open_overviews), cap(count_open_overviews)) }

        if OV_handler.SetOpen(hid, ovfh) {
            //reply.ovfh = ovfh
            //reply.retbool = true
            //retval = reply.retbool
            //opened++
            return ovfh, true
        } else {
            log.Printf("FATAL ERROR (OPENER) OV_H %d) OV_handler.SetOpen failed!", hid)
            OV_handler.KILL(hid)
        }
    }

    // was not open, return empty OVFH struct and true, will create new file handle
    log.Printf("OV_H %d) ERROR GetOpen failed final hash='%s'", hid, hash)
    return OVFH{}, false
} // end func OV_Handler.GetOpen


func (c *OV_Handler) SetOpen(hid int, ovfh OVFH) bool {
    retval := false
    if c.Debug { log.Printf("OV_H %d) SetOpen hash='%s'", hid, ovfh.Hash) }

    c.mux.Lock()
    mapdata := c.V[ovfh.Hash]
    //if mapdata.hid == hid && mapdata.preopen && !mapdata.open {
    if mapdata.hid == hid /* && mapdata.preopen && !mapdata.open */ {
        mapdata.open = true
        mapdata.preopen = false
        mapdata.ovfh = ovfh
        mapdata.idle = -1 // while any worker is working set idle to -1
        retval = true
        c.V[ovfh.Hash] = mapdata
    } else {
        log.Printf("ERROR OV_H %d) SetOpen invalid mapdata.hid=%d open=%t preopen=%t idle=%d", hid, mapdata.hid, mapdata.open, mapdata.preopen, mapdata.idle)
        OV_handler.KILL(hid)
    }
    c.mux.Unlock()
    return retval
} // end func OV_Handler.SetOpen


func (c *OV_Handler) Park(hid int, ovfh OVFH) bool {
    retval := false
    c.mux.Lock()
    mapdata := c.V[ovfh.Hash]
    //if mapdata.hid == -1 || mapdata.hid >= 0 && mapdata.open && !mapdata.preopen {
    if mapdata.hid >= 0 && mapdata.open && !mapdata.preopen {
        mapdata.hid = 0 // un-assign hid from overview file
        mapdata.ovfh = ovfh // carries updated values we have to check to flush and close idle ones
        mapdata.idle = utils.Now()
        c.V[ovfh.Hash] = mapdata
        retval = true
    } else {
        log.Printf("FATAL ERROR OV_H %d) Park mapdata.hid=%d mapdata.open=%t mapdata.preopen=%t hash='%s'", hid, mapdata.hid, mapdata.open, mapdata.preopen, ovfh.Hash)
        OV_handler.KILL(hid)
    }
    c.mux.Unlock()

    return retval
} // end func OV_Handler.Park


func (c *OV_Handler) Check_idle() {
    //sent_stop := make(map[string]bool, MAX_OPEN_MMAPS)
    isleep_min, isleep_max, isleep := 5, 100, 100
    forever:
    for {
        c.mux.Lock()
        len_cv := len(c.V)
        c.mux.Unlock()

        if len_cv < OV_handler.MAX_OPEN_MMAPS/2 {
            isleep += 1
        } else {
            isleep -= 2
        }
        if isleep < isleep_min {
            isleep = isleep_min
        } else
        if isleep > isleep_max {
            isleep = isleep_max
        }

        need_stop := is_closed_server(c.STOP)
        if need_stop {
            if len_cv == 0 {
                log.Printf("OV Check_idle len_cv=0 need_stop=%t", need_stop)
                break forever
            }
            time.Sleep(1 * time.Millisecond)
        } else {
            time.Sleep(time.Duration(isleep) * time.Millisecond)
            //continue forever
        }

        start := utils.Nano()

        // find one we can flush and close
        send_close := false
        var newdata OV_Handler_data
        var close_request Overview_Close_Request
        //var close_requests []Overview_Close_Request
        looped := 0

        c.mux.Lock()

        find_one:
        for hash, data := range c.V {
            looped++

            if hash != data.ovfh.Hash {
                if data.ovfh.Hash == "" { // not yet set?
                    if c.Debug { log.Printf("WARN OV Check_idle: data.ovfh.Hash is empty, not yet set? hash='%s'", hash) }
                    continue find_one
                } else {
                    log.Printf("FATAL ERROR OV Check_idle hash='%s' != data.ovfh.Hash='%s'", hash, data.ovfh.Hash)
                    OV_handler.KILL(0)
                    c.mux.Unlock()
                    break forever
                }
            }

            var lastflush int64
            if data.ovfh.Time_flush > 0 {
                lastflush = utils.Now() - data.ovfh.Time_flush
            }

            if data.hid == 0 && (need_stop || lastflush > MAX_FLUSH) {
                if open_mmap_overviews.closelockMMAP(0, hash) {
                    close_request.force_close = true
                    close_request.ovfh = data.ovfh
                    close_request.reply_chan = nil
                    newdata = data
                    newdata.hid = -2
                    send_close = true
                    time_open := utils.Now() - data.ovfh.Time_open
                    written := data.ovfh.Written
                    if c.Debug || lastflush > MAX_FLUSH*2 { log.Printf("OV Check_Idle: close lastflush=%d open=%d written=%d len_cv=%d hash='%s'", lastflush, time_open, written, len(c.V), hash) }
                    break find_one
                }
            }

        } // end for find_one

        if send_close {
            c.V[newdata.ovfh.Hash] = newdata
        }
        c.mux.Unlock()

        if send_close {
            close_request_chan <- close_request
            //sent_stop[newdata.ovfh.Hash] = true
        }

        /*
        find_one:
        for hash, data := range c.V {
            looped++
            if hash != data.ovfh.Hash {
                if data.ovfh.Hash == "" { // not yet set?
                    continue find_one
                } else {
                    log.Printf("FATAL ERROR OV_Handler Check_idle hash='%s' != data.ovfh.Hash='%s'", hash, data.ovfh.Hash)
                    OV_handler.KILL(0)
                    break forever
                }
            }

            if need_stop && sent_stop[hash] {
                continue find_one
            }
            lastflush := utils.Now() - data.ovfh.Time_flush
            if need_stop || (data.hid == 0 && data.open && !data.preopen) {

                var idle int64
                if data.idle > 0 {
                    idle = utils.Now() - data.idle
                }


                //if c.Debug && need_stop {
                //    log.Printf("OV_Handler Check_Idle data.hid=%d idle=%d written=%d lastflush=%d hash='%s'", data.hid, idle, data.ovfh.Written, lastflush, hash)
                //}


                // hid has to be 0.
                // -2 == is closing
                // -1 == is flushing
                // 0 == not assigned
                // >0 == assigned to handler id
                if data.hid == 0 && (need_stop || lastflush >= MAX_FLUSH) {

                    logcmd, logdatahid := "", -999
                    if need_stop {
                        // update to force_close request because mmap is idle
                        close_request.force_close = true
                        data.hid = -2 // set force_close flag
                        logdatahid = data.hid
                        send_close = true
                        logcmd = "force_close"
                        if need_stop {
                            sent_stop[hash] = true
                        }
                    } else
                    //if lastflush >= MAX_FLUSH || close_request.ovfh.Written > maxwritten  {
                    if lastflush >= MAX_FLUSH  {
                        data.hid = -1 // set flush flag
                        logdatahid = data.hid
                        send_close = true
                        logcmd = "flush"
                    }
                    if c.Debug || logcmd != "" { log.Printf("OV_Handler Check_Idle logcmd=%s data.hid=%d need_stop=%t idle=%d lastflush=%d written=%d hash='%s'", logcmd, logdatahid, need_stop, idle, lastflush, close_request.ovfh.Written, hash) }


                    if send_close {
                        newdata = data
                        close_request.ovfh = data.ovfh
                        close_request.reply_chan = nil
                        close_request_chan <- close_request
                    } // end if send_close

                } // end if need_stop || lastflush
            }
        } // end for find_one
        */
        /*
        if newdata.hid < 0 && send_close { // will be set to -1 if mmap is in flush or -2 if is force closing
            c.V[newdata.ovfh.Hash] = newdata
        }
        c.mux.Unlock()
        */
        if c.Debug { log.Printf("idle_loop looped=%d took=%d ns", looped, utils.Nano()-start) }
        /*
        if send_close {
            close_request_chan <- close_request
        }*/
    } // end for forever
} // end func OV_Handler.Check_idle_Loop


func (c *OV_Handler) Is_assigned(hash string) bool {
    retval := false
    c.mux.Lock()
    data := c.V[hash]
    c.mux.Unlock()
    if data.hid < 0 || data.hid > 0  { // is 0 if not assigned, -1 flush, -2 force_close
        retval = true
    }
    return retval
} // end func OV_Handler.Is_assigned


func (c *OV_Handler) Is_open(hash string) bool {
    retval := false
    c.mux.Lock()
    data := c.V[hash]
    c.mux.Unlock()
    if data.ovfh.Time_open > 0 {
        retval = true
    }
    return retval
} // end func OV_Handler.Is_open

func (c *OV_Handler) DelHandle(hash string) {
    c.mux.Lock()
    delete(c.V, hash)
    c.mux.Unlock()
} // end func OV_Handler.DelHandle


func (c *OV_Handler) Overview_handler_OPENER(hid int, open_request_chan chan Overview_Open_Request) {
    // waits for requests to open or retrieve already open mmap handle
    if c.Debug { log.Printf("OV OPENER %d) START", hid) }
    var opened uint64
    var open_errors uint64
    loops := 0

    forever:
    for {

        // before open any mmap check if we should stop
        if is_closed_server(c.STOP) && len(open_request_chan) == 0 {
            loops++
            time.Sleep(100 * time.Millisecond)
            if loops > 10 {
                if c.Debug { log.Printf("OV OPENER %d) got STOP signal", hid) }
                break forever
            }
        }

        select_one: // select only one to open because we dont know here if we can open more
        select {

            case _, ok := <- c.STOP:
                if !ok && len(open_request_chan) == 0 {
                    if c.Debug { log.Printf("OV OPENER %d) STOPPING", hid) }
                    break forever
                }

            case open_request, ok := <- open_request_chan:
                if !ok {
                    log.Printf("OV OPENER %d) open_request_chan is closed", hid)
                    break forever
                }
                // got open_request for overview file ( Overview_Open_Request = { hash, file_path, reply_chan } )
                if OV_handler.process_open_request(hid, open_request) {
                    opened++
                } else {
                    open_errors++
                }
                break select_one

        } // end select_one

    } // end for forever OPENER

    if open_errors > 0 {
        log.Printf("ERROR OV OPENER %d) returned opened=%d open_errors=%d", hid, opened, open_errors)
    }
} // end func Overview_handler_OPENER


func (c *OV_Handler) Overview_handler_CLOSER(hid int, close_request_chan chan Overview_Close_Request) {
    // waits for requests to flush / park or force_close a mmap handle
    if c.Debug { log.Printf("OV CLOSER %d) START", hid) }
    var closed uint64
    var close_errors uint64

    forever:
    for {
        for_closer:
        for {
            select {

                case _, ok := <- c.STOP:
                    if !ok && len(open_request_chan) == 0 && len(count_open_overviews) == 0 {
                        if c.Debug { log.Printf("OV CLOSER %d) STOPPING", hid) }
                        break forever
                    }

                case close_request, ok := <- close_request_chan:
                    if !ok {
                        time.Sleep(1 * time.Second/10)
                        break for_closer
                    }
                    // got close_request for overview file ( Overview_Close_Request = { ovfh, reply_chan } )
                    if OV_handler.process_close_request(hid, close_request) {
                        closed++
                    } else {
                        log.Printf("OV_handler.process_close_request returned false")
                        close_errors++
                    }
                /*
                default:
                    // no more close requests
                    break for_closer
                */
            }
        }

        /*
        if is_closed_server(c.STOP) {
            if c.Debug { log.Printf("OV_handler %d) got STOP signal", hid) }
            open_overviews := len(count_open_overviews)
            if open_overviews == 0 && len(open_request_chan) == 0 {
                log.Printf("OV_handler STOP open_overviews=%d", open_overviews)
                break forever
            }
            if c.Debug { log.Printf("OV_handler %d) WAIT STOP open_overviews=%d != 0", hid, open_overviews) }
            time.Sleep(1 * time.Second/10)
            continue forever
        }
        */

    } // end for forever CLOSER

    if close_errors > 0 {
        log.Printf("ERROR OV CLOSER %d) returned closed=%d close_errors=%d", hid, closed, close_errors)
    }
} // end func Overview_handler_CLOSER


func (c *OV_Handler) open_overviews() int {
    return len(count_open_overviews)
}


func (c *OV_Handler) process_close_request(hid int, close_request Overview_Close_Request) bool {
    retval := false
    //force_close := true

    force_close := is_closed_server(c.STOP)
    open_overviews := len(count_open_overviews)
    if !force_close && ( close_request.force_close || open_overviews == c.MAX_OPEN_MMAPS ) {
        // always close mmap if force_close is set or max_open_maps is reached
        force_close = true
    }

    file := filepath.Base(close_request.ovfh.File_path)
    if c.Debug { log.Printf("(CLOSER) OV_H %d) close_request force_close=%t fp='%s'", hid, force_close, file) }

    var reply Overview_Reply

    if !force_close {
        // only flush here if not forced to close file as close will flush anyways
        lastflush := utils.Now() - close_request.ovfh.Time_flush
        wrote := close_request.ovfh.Written

        if lastflush >= MAX_FLUSH && wrote <= OV_RESERVE_END {
            // map was idle since previous lastflush
            // if written==OV_RESERVE_END: file has only updated the footer and 0 writes since prev flush
            // force_close mmap.
            force_close = true
        } else
        if lastflush >= MAX_FLUSH && wrote > OV_RESERVE_END {
            if ovfh, err := Update_Footer(close_request.ovfh); err != nil {
                log.Printf("FATAL ERROR (CLOSER) OV_H %d) process_close_request Update_Footer failed err='%v' fp='%s", hid, err, file)
                reply.err = err
                OV_handler.KILL(hid)
            } else {
                if c.Debug { log.Printf("(CLOSER) OV_H %d) process_close_request flushed lastflush=%d fp='%s", hid, lastflush, file) }
                close_request.ovfh = ovfh
            }
            // mmap needs a flush

            if err := Flush_ov(close_request.ovfh); err != nil {
                // flush failed
                log.Printf("FATAL ERROR (CLOSER)  OV_H %d) Flush_ov failed fp='%s", hid, file)
                reply.err = err
                OV_handler.KILL(hid)
            } else {
                // overview file flushed ok
                close_request.ovfh.Time_flush = utils.Now()
                close_request.ovfh.Written -= wrote

            }// end Flush_ov
        } // end if lastflush
    } // end if !force_close


    if !force_close && reply.err == nil {
        retval = OV_handler.Park(hid, close_request.ovfh)

    } else
    // reply.err should be nil if flush ran before and was ok
    if force_close || reply.err == nil {
        // really close the file, return retbool and err to reply_chan
        update_footer, grow := true, false
        if err := handle_close_ov(close_request.ovfh, update_footer, force_close, grow); err == nil {
            <- count_open_overviews // suck one out reduces len of channel
            retval = true
            reply.retbool = retval
            //if c.Debug { log.Printf("(CLOSER) OV_H %d) open_overviews=%d sucked one", hid, open_overviews) }
            /*
            open_overviews := len(count_open_overviews)
            if open_overviews > 0 {
                if c.Debug { log.Printf("(CLOSER) OV_H %d) open_overviews=%d wait suck", hid, open_overviews) }
                <- count_open_overviews // suck one out reduces len of channel
                if c.Debug { log.Printf("(CLOSER) OV_H %d) open_overviews=%d sucked one", hid, open_overviews) }
                reply.retbool = true
                retval = reply.retbool
                //closed++
            } else {
                log.Printf("ERROR (CLOSER) OV_H %d) failed to suck one out but handle_close_ov returned true?!", hid)
            }
            */
        } else {
            log.Printf("ERROR (CLOSER) OV_H %d) Close_ov err='%v' fp='%s'", hid, err, file)
            reply.err = err
            //close_errors++
        }
    } // end if reply.err == nil

    if force_close {
        OV_handler.DelHandle(close_request.ovfh.Hash)
    }

    if retval || force_close {
        open_mmap_overviews.unlockMMAP(hid, force_close, close_request.ovfh.Hash)
    }

    if close_request.reply_chan != nil {
        close_request.reply_chan <- reply
        close(close_request.reply_chan) // FIXME: mark*836b04be* do not close here if we want to reuse later! mark*836b04be*
    }

    return retval
} // end func process_close_request


func (c *OV_Handler) process_open_request(hid int, open_request Overview_Open_Request) bool {
    retval := false
    /*
    if is_closed_server(c.STOP) {
        return false
    }*/
    var reply Overview_Reply
    //if c.Debug { log.Printf("OV_handler %d) open_request hash='%s'", hid, open_request.hash) }
    if open_request.hash == "" || len(open_request.hash) != 64 {
        reply.err = fmt.Errorf("ERROR (OPENER) OV_H open_request len_hash=%d", len(open_request.hash))
    }

    if reply.err == nil {
        // check if hash is already open and return the ovfh handle
        if ovfh, retbool := OV_handler.GetOpen(hid, open_request.file_path, open_request.hash); retbool == true {
            if ovfh.Time_open == 0 {
                // did not get an open overview handle
            } else {
                reply.ovfh = ovfh
                retval = true
            }
        }
    }


    if reply.err == nil && reply.ovfh.Time_open == 0 {
        /*
        open_overviews := len(count_open_overviews)
        if open_overviews == MAX_OPEN_MMAPS {
            // re-queue the request?
            //open_request_chan <- open_request
            log.Printf("(OPENER) OV_H %d) open_overviews=%d reached MAX_OPEN_MMAPS=%d", hid, open_overviews, MAX_OPEN_MMAPS)
            time.Sleep(1 * time.Second)
            return OV_handler.process_open_request(hid, open_request)
            //reply.err = fmt.Errorf("ERROR (OPENER) OV_H %d) MAX_OPEN_MMAPS=%d reached err='%v'", hid, MAX_OPEN_MMAPS, reply.err)
        }

        if reply.err == nil {
            // file was not open. open the file and return the handle to reply_chan or err
            if c.Debug { log.Printf("(OPENER) OV_H %d) open_request fp='%s'", hid, filepath.Base(open_request.file_path)) }

            if ovfh, err := handle_open_ov(open_request.hash, open_request.file_path); err != nil {
                log.Printf("ERROR (OPENER) OV_H %d) Open_ov err='%v' fp='%s'", hid, err, filepath.Base(open_request.file_path))
                reply.err = err
                //open_errors++
            } else {
                if OV_handler.SetOpen(hid, ovfh) {
                    reply.ovfh = ovfh
                    reply.retbool = true
                    retval = reply.retbool
                    //opened++
                } else {
                    log.Printf("FATAL ERROR (OPENER) OV_H %d) OV_handler.SetOpen failed!", hid)
                    OV_handler.KILL(hid)
                }
            }
        } // end if reply.err == nil
        */

    } // end if reply.err == nil

    open_request.reply_chan <- reply
    close(open_request.reply_chan) // FIXME: mark*836b04be* do not close here if we want to reuse later! mark*836b04be*

    return retval
} // end func process_open_request


func (c *OV_Handler) KILL(hid int) {
    log.Printf("ERROR OV_H %d) KILL", hid)
    c.STOP <- true
}
