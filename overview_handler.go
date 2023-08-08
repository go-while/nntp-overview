package overview

import (
	"fmt"
	"github.com/go-while/go-utils"
	"log"
	"path/filepath"
	"sync"
	"time"
)

type OV_Handler_data struct {
	ovfh    *OVFH // overview mmap file handle struct
	hid     int   // overview mmap is assigned to handler id
	preopen bool  // overview mmap is opening
	open    bool  // overview mmap is open
	idle    int64 // overview mmap is idle since unix timestamp
}

type OV_Handler struct {
	V              map[string]OV_Handler_data
	Debug          bool
	mux            sync.Mutex
	STOP           chan bool
	MAX_OPEN_MMAPS int
	CLOSE_ALWAYS   bool
}

func (oh *OV_Handler) GetOpen(hid int, who *string, file_path string, hash string) (*OVFH, bool) {

	if retbool, ch := open_mmap_overviews.lockMMAP(hid, who, hash, Overview.signal_chans[hid]); retbool == false && ch != nil {
		<-ch // wait for anyone to return the unlock for this group
	} else if retbool == false && ch == nil {
		// could not lock and didnt return a signal channel? should never happen he says!
		log.Printf("%s WARN GetOpen lockMMAP retry hash='%s'", *who, hash)
		time.Sleep(1 * time.Millisecond)
		return OV_handler.GetOpen(hid, who, file_path, hash)
	} else {
		if oh.Debug {
			log.Printf("%s TRUE GetOpen -> lockMMAP retbool=%t hash='%s'", *who, retbool, hash)
		}
	}

	oh.mux.Lock()

	mapdata := oh.V[hash]

	if mapdata.hid < 0 {
		oh.mux.Unlock()
		log.Printf("%s GetOpen retry isin flush/force_close? hid=%d hash='%s'", *who, mapdata.hid, hash)
		time.Sleep(1 * time.Millisecond)
		return OV_handler.GetOpen(hid, who, file_path, hash)
	}

	if mapdata.hid > 0 && mapdata.hid != hid {
		oh.mux.Unlock()
		log.Printf("%s GetOpen retry isassigned hid=%d hash='%s'", *who, mapdata.hid, hash)
		time.Sleep(1 * time.Millisecond)
		return OV_handler.GetOpen(hid, who, file_path, hash)
	}

	if !utils.FileExists(file_path) {
		if err := Create_ov(who, file_path, hash, 3); err != nil {
			oh.mux.Unlock()
			log.Printf("%s FATAL ERROR Create_ov hash='%s' err='%v'", *who, hash, err)
			OV_handler.KILL(who)
			return nil, false
		}
		if oh.Debug {
			log.Printf("%s Create_ov hash='%s' OK", *who, hash)
		}
	}

	if oh.Debug {
		log.Printf("%s OK GetOpen after lockMMAP hash='%s'", *who, hash)
	}

	// was not assigned, assign now to us
	mapdata.hid = hid

	if mapdata.open && !mapdata.preopen {
		ovfh := mapdata.ovfh
		oh.V[hash] = mapdata // updates the hid
		oh.mux.Unlock()
		if oh.Debug {
			log.Printf("%s GetOpen returned mapdata.open=true hash='%s'", *who, hash)
		}
		return ovfh, true
	}

	mapdata.preopen = true
	oh.V[hash] = mapdata
	oh.mux.Unlock()

	if ovfh, err := handle_open_ov(who, hash, file_path); err != nil {
		log.Printf("%s ERROR (OPENER) Open_ov err='%v' fp='%s'", *who, err, filepath.Base(file_path))
		//reply.err = err
		//open_errors++
	} else {

		if oh.Debug {
			log.Printf("%s GetOpen before count_open_overviews=%d/%d", *who, len(count_open_overviews), cap(count_open_overviews))
		}
		count_open_overviews <- struct{}{} // pass an empty struct into the open_file counter channel
		if oh.Debug {
			log.Printf("%s GetOpen afters count_open_overviews=%d/%d", *who, len(count_open_overviews), cap(count_open_overviews))
		}

		if OV_handler.SetOpen(hid, who, ovfh) {
			log.Printf("%s SetOpen TRUE fp='%s'", *who, filepath.Base(file_path))
			//reply.ovfh = ovfh
			//reply.retbool = true
			//retval = reply.retbool
			//opened++
			return ovfh, true
		} else {
			log.Printf("%s FATAL ERROR (OPENER) OV_handler.SetOpen failed!", *who)
			OV_handler.KILL(who)
		}
	}

	// was not open, return empty OVFH struct and true, will create new file handle
	log.Printf("%s ERROR GetOpen failed final hash='%s'", hid, hash)
	return nil, false
} // end func OV_Handler.GetOpen

func (oh *OV_Handler) SetOpen(hid int, who *string, ovfh *OVFH) bool {
	retval := false
	if oh.Debug {
		log.Printf("%s SetOpen hash='%s'", *who, ovfh.Hash)
	}

	oh.mux.Lock()
	mapdata := oh.V[ovfh.Hash]
	//if mapdata.hid == hid && mapdata.preopen && !mapdata.open {
	if mapdata.hid == hid /* && mapdata.preopen && !mapdata.open */ {
		mapdata.open = true
		mapdata.preopen = false
		mapdata.ovfh = ovfh
		mapdata.idle = -1 // while any worker is working set idle to -1
		retval = true
		oh.V[ovfh.Hash] = mapdata
	} else {
		log.Printf("%s ERROR SetOpen invalid mapdata.hid=%d open=%t preopen=%t idle=%d", *who, mapdata.hid, mapdata.open, mapdata.preopen, mapdata.idle)
		OV_handler.KILL(who)
	}
	oh.mux.Unlock()
	return retval
} // end func OV_Handler.SetOpen

func (oh *OV_Handler) Park(hid int, who *string, ovfh *OVFH) bool {
	retval := false
	oh.mux.Lock()
	mapdata := oh.V[ovfh.Hash]
	//if mapdata.hid == -1 || mapdata.hid >= 0 && mapdata.open && !mapdata.preopen {
	if mapdata.hid >= 0 && mapdata.open && !mapdata.preopen {
		mapdata.hid = 0     // un-assign hid from overview file
		mapdata.ovfh = ovfh // carries updated values we have to check to flush and close idle ones
		mapdata.idle = utils.Now()
		oh.V[ovfh.Hash] = mapdata
		retval = true
	} else {
		log.Printf("%s FATAL ERROR Park mapdata.hid=%d mapdata.open=%t mapdata.preopen=%t hash='%s'", *who, hid, mapdata.hid, mapdata.open, mapdata.preopen, ovfh.Hash)
		OV_handler.KILL(who)
	}
	oh.mux.Unlock()

	return retval
} // end func OV_Handler.Park

func (oh *OV_Handler) Check_idle() {
	isleep := 250 // milliseconds
	who := "Check_idle()"
	for {
		time.Sleep(time.Duration(isleep) * time.Millisecond)

		need_stop := is_closed_server(oh.STOP)
		if need_stop {
			oh.mux.Lock()
			if len(oh.V) == 0 {
				log.Printf("OV Check_idle len_cv=0 need_stop=%t", need_stop)
				oh.mux.Unlock()
				return
			}
			oh.mux.Unlock()
			time.Sleep(1 * time.Millisecond)
		}

		//start := utils.Nano()
		// find open overviews we can flush and close
		var newdata OV_Handler_data
		var close_request Overview_Close_Request
		var close_requests []Overview_Close_Request

		oh.mux.Lock()
	find_idles:
		for hash, data := range oh.V {
			if data.ovfh == nil { // not yet set?
				log.Printf("%s WARN OV Check_idle: data.ovfh=nil, not yet set? hash='%s'", who, hash)
				continue
			}
			if hash != data.ovfh.Hash {
				if data.ovfh.Hash == "" { // not yet set?
					if oh.Debug {
						log.Printf("%s WARN OV Check_idle: data.ovfh.Hash is empty, not yet set? hash='%s'", who, hash)
					}
					continue find_idles
				} else {
					log.Printf("%s FATAL ERROR OV Check_idle hash='%s' != data.ovfh.Hash='%s'", who, hash, data.ovfh.Hash)
					OV_handler.KILL(&who)
					oh.mux.Unlock()
					return
				}
			}

			var lastflush int64
			if data.ovfh.Time_flush > 0 {
				lastflush = utils.Now() - data.ovfh.Time_flush
			}

			if data.hid == 0 && (lastflush >= MAX_FLUSH || need_stop) {
				if open_mmap_overviews.closelockMMAP(0, hash) {
					close_request.force_close = true
					close_request.ovfh = data.ovfh
					close_request.reply_chan = nil
					close_requests = append(close_requests, close_request)
					newdata = data
					newdata.hid = -2
					time_open := utils.Now() - data.ovfh.Time_open
					written := data.ovfh.Written
					oh.V[newdata.ovfh.Hash] = newdata
					if oh.Debug || lastflush > MAX_FLUSH*2 {
						log.Printf("%s OV Check_Idle: close hash='%s' lastflush=%d time_open=%d written=%d len(oh.V)=%d", who, hash, lastflush, time_open, written, len(oh.V))
					}
					//break find_one
				}
			}

		} // end for find_idles
		oh.mux.Unlock()

		for _, close_request := range close_requests {
			close_request_chan <- close_request
		}

		if oh.Debug {
			//log.Printf("OV_Handler.Check_idle_Loop took=%d ns", utils.Nano()-start)
		}
	} // end for forever

} // end func OV_Handler.Check_idle

func (oh *OV_Handler) Is_assigned(hash string) bool {
	retval := false
	oh.mux.Lock()
	data := oh.V[hash]
	oh.mux.Unlock()
	if data.hid < 0 || data.hid > 0 { // is 0 if not assigned, -1 flush, -2 force_close
		retval = true
	}
	return retval
} // end func OV_Handler.Is_assigned

func (oh *OV_Handler) Is_open(hash string) bool {
	retval := false
	oh.mux.Lock()
	data := oh.V[hash]
	oh.mux.Unlock()
	if data.ovfh.Time_open > 0 {
		retval = true
	}
	return retval
} // end func OV_Handler.Is_open

func (oh *OV_Handler) DelHandle(hash string) {
	oh.mux.Lock()
	delete(oh.V, hash)
	oh.mux.Unlock()
} // end func OV_Handler.DelHandle

func (oh *OV_Handler) Overview_handler_OPENER(hid int, open_request_chan chan Overview_Open_Request) {
	// waits for requests to open or retrieve already open mmap handle
	if oh.Debug {
		log.Printf("OV OPENER %d) START", hid)
	}
	who := fmt.Sprintf("OV:O:%d", hid)
	var opened uint64
	var open_errors uint64

for_opener:
	for {
		select {
		case open_request, ok := <-open_request_chan:
			if !ok {
				log.Printf("OV OPENER %d) open_request_chan closed", hid)
				break for_opener
			}
			// got open_request for overview file ( Overview_Open_Request = { hash, file_path, reply_chan } )
			if OV_handler.process_open_request(hid, &who, open_request) {
				opened++
			} else {
				open_errors++
			}

		} // end select_one

	} // end for forever OPENER

	if open_errors > 0 {
		log.Printf("ERROR OV OPENER %d) returned opened=%d open_errors=%d", hid, opened, open_errors)
	}
} // end func Overview_handler_OPENER

func (oh *OV_Handler) Overview_handler_CLOSER(hid int, close_request_chan chan Overview_Close_Request) {
	// waits for requests to flush / park or force_close a mmap handle
	if oh.Debug {
		log.Printf("OV CLOSER %d) START", hid)
	}
	who := fmt.Sprintf("OV:C:%d", hid)
	var closed uint64
	var close_errors uint64

for_closer:
	for {
		select {
		case close_request, ok := <-close_request_chan:
			if !ok {
				log.Printf("%s OV CLOSER close_request_chan closed", who)
				break for_closer
			}
			// got close_request for overview file ( Overview_Close_Request = { ovfh, reply_chan } )
			if OV_handler.process_close_request(hid, &who, close_request) {
				closed++
			} else {
				log.Printf("%s OV_handler.process_close_request returned false", who)
				close_errors++
			}
		}
	}

	if close_errors > 0 {
		log.Printf("%s ERROR OV CLOSER returned closed=%d close_errors=%d", who, closed, close_errors)
	}
} // end func Overview_handler_CLOSER

/*
func (oh *OV_Handler) open_overviews() int {
	return len(count_open_overviews)
}
*/

func (oh *OV_Handler) process_close_request(hid int, who *string, close_request Overview_Close_Request) bool {
	retval := false
	//force_close := true

	force_close := is_closed_server(oh.STOP)
	//open_overviews := len(count_open_overviews)
	if !force_close && (oh.CLOSE_ALWAYS || close_request.force_close || len(count_open_overviews) == oh.MAX_OPEN_MMAPS) {
		// always close mmap if force_close is set or max_open_maps is reached
		force_close = true
	}

	file := filepath.Base(close_request.ovfh.File_path)
	if oh.Debug {
		log.Printf("(CLOSER) %s close_request force_close=%t fp='%s'", *who, force_close, file)
	}

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
		} else if lastflush >= MAX_FLUSH && wrote > OV_RESERVE_END {
			if new_ovfh, err := Update_Footer(who, close_request.ovfh, "OV_H:process_close_request"); err != nil {
				log.Printf("FATAL ERROR (CLOSER) %s process_close_request Update_Footer failed err='%v' fp='%s", *who, err, file)
				reply.err = err
				OV_handler.KILL(who)
			} else {
				if oh.Debug {
					log.Printf("(CLOSER) %s process_close_request flushed lastflush=%d fp='%s", *who, lastflush, file)
				}
				if new_ovfh != nil && new_ovfh.Mmap_handle != nil {
					close_request.ovfh = new_ovfh
				}
			}
			// mmap needs a flush

			if err := Flush_ov(who, close_request.ovfh); err != nil {
				// flush failed
				log.Printf("FATAL ERROR (CLOSER)  %s Flush_ov failed fp='%s", *who, file)
				reply.err = err
				OV_handler.KILL(who)
			} else {
				// overview file flushed ok
				close_request.ovfh.Time_flush = utils.Now()
				close_request.ovfh.Written -= wrote

			} // end Flush_ov
		} // end if lastflush
	} // end if !force_close

	if !force_close && reply.err == nil {
		retval = OV_handler.Park(hid, who, close_request.ovfh)

	} else
	// reply.err should be nil if flush ran before and was ok
	if force_close || reply.err == nil {
		// really close the file, return retbool and err to reply_chan
		update_footer, grow := true, false
		if err := handle_close_ov(who, close_request.ovfh, update_footer, force_close, grow); err == nil {
			<-count_open_overviews // suck one out reduces len of channel
			retval = true
			reply.retbool = retval
		} else {
			log.Printf("ERROR (CLOSER) %s Close_ov err='%v' fp='%s'", *who, err, file)
			reply.err = err
			//close_errors++
		}
	} // end if reply.err == nil

	if force_close {
		OV_handler.DelHandle(close_request.ovfh.Hash)
	}

	if retval || force_close {
		open_mmap_overviews.unlockMMAP(hid, who, force_close, close_request.ovfh.Hash)
	}

	if close_request.reply_chan != nil {
		close_request.reply_chan <- reply
		close(close_request.reply_chan) // FIXME: mark*836b04be* do not close here if we want to reuse later! mark*836b04be*
	}

	return retval
} // end func process_close_request

func (oh *OV_Handler) process_open_request(hid int, who *string, open_request Overview_Open_Request) bool {
	retval := false
	/*
		if is_closed_server(oh.STOP) {
			return false
		}*/
	var reply Overview_Reply
	//if oh.Debug { log.Printf("OV_handler %d) open_request hash='%s'", hid, open_request.hash) }
	if open_request.hash == "" || len(open_request.hash) != 64 {
		reply.err = fmt.Errorf("ERROR (OPENER) OV_H open_request len_hash=%d", len(open_request.hash))
	}

	if reply.err == nil {
		// check if hash is already open and return the ovfh handle
		if ovfh, retbool := OV_handler.GetOpen(hid, who, open_request.file_path, open_request.hash); retbool == true {
			if ovfh.Time_open == 0 {
				// did not get an open overview handle
			} else {
				reply.ovfh = ovfh
				retval = true
			}
		}
	}

	open_request.reply_chan <- reply
	close(open_request.reply_chan) // FIXME: mark*836b04be* do not close here if we want to reuse later! mark*836b04be*

	return retval
} // end func process_open_request

func (oh *OV_Handler) KILL(who *string) {
	log.Printf("ERROR %s KILL", *who)
	oh.STOP <- true
}
