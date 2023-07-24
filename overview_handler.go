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
	ovfh    OVFH  // overview mmap file handle struct
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

func (oh *OV_Handler) GetOpen(hid int, file_path string, hash string) (OVFH, bool) {

	if retbool, ch := open_mmap_overviews.lockMMAP(hid, hash, Overview.signal_chans[hid]); retbool == false && ch != nil {
		<-ch // wait for anyone to return the unlock for this group
	} else if retbool == false && ch == nil {
		// could not lock and didnt return a signal channel? should never happen he says!
		log.Printf("WARN OV_H %d) GetOpen lockMMAP retry hash='%s'", hid, hash)
		time.Sleep(1 * time.Millisecond)
		return OV_handler.GetOpen(hid, file_path, hash)
	} else {
		if oh.Debug {
			log.Printf("TRUE OV_H %d) GetOpen -> lockMMAP retbool=%t hash='%s'", hid, retbool, hash)
		}
	}

	oh.mux.Lock()

	mapdata := oh.V[hash]

	if mapdata.hid < 0 {
		oh.mux.Unlock()
		log.Printf("OV_H %d) GetOpen retry isin flush/force_close? hid=%d hash='%s'", hid, mapdata.hid, hash)
		time.Sleep(1 * time.Millisecond)
		return OV_handler.GetOpen(hid, file_path, hash)
	}

	if mapdata.hid > 0 && mapdata.hid != hid {
		oh.mux.Unlock()
		log.Printf("OV_H %d) GetOpen retry isassigned hid=%d hash='%s'", hid, mapdata.hid, hash)
		time.Sleep(1 * time.Millisecond)
		return OV_handler.GetOpen(hid, file_path, hash)
	}

	if !utils.FileExists(file_path) {
		if err := Create_ov(file_path, hash, 3); err != nil {
			oh.mux.Unlock()
			log.Printf("FATAL ERROR OV_H %d) Create_ov hash='%s' err='%v'", hid, hash, err)
			OV_handler.KILL(hid)
			return OVFH{}, false
		}
		if oh.Debug {
			log.Printf("OV_H %d) Create_ov hash='%s' OK", hid, hash)
		}
	}

	if oh.Debug {
		log.Printf("OV_H %d) OK GetOpen after lockMMAP hash='%s'", hid, hash)
	}

	// was not assigned, assign now to us
	mapdata.hid = hid

	if mapdata.open && !mapdata.preopen {
		ovfh := mapdata.ovfh
		oh.V[hash] = mapdata // updates the hid
		oh.mux.Unlock()
		if oh.Debug {
			log.Printf("OV_H %d) GetOpen returned mapdata.open=true hash='%s'", hid, hash)
		}
		return ovfh, true
	}

	mapdata.preopen = true
	oh.V[hash] = mapdata
	oh.mux.Unlock()

	if ovfh, err := handle_open_ov(hash, file_path); err != nil {
		log.Printf("ERROR (OPENER) OV_H %d) Open_ov err='%v' fp='%s'", hid, err, filepath.Base(file_path))
		//reply.err = err
		//open_errors++
	} else {

		if oh.Debug {
			log.Printf("OV_H %d) GetOpen before count_open_overviews=%d/%d", hid, len(count_open_overviews), cap(count_open_overviews))
		}
		count_open_overviews <- struct{}{} // pass an empty struct into the open_file counter channel
		if oh.Debug {
			log.Printf("OV_H %d) GetOpen afters count_open_overviews=%d/%d", hid, len(count_open_overviews), cap(count_open_overviews))
		}

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

func (oh *OV_Handler) SetOpen(hid int, ovfh OVFH) bool {
	retval := false
	if oh.Debug {
		log.Printf("OV_H %d) SetOpen hash='%s'", hid, ovfh.Hash)
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
		log.Printf("ERROR OV_H %d) SetOpen invalid mapdata.hid=%d open=%t preopen=%t idle=%d", hid, mapdata.hid, mapdata.open, mapdata.preopen, mapdata.idle)
		OV_handler.KILL(hid)
	}
	oh.mux.Unlock()
	return retval
} // end func OV_Handler.SetOpen

func (oh *OV_Handler) Park(hid int, ovfh OVFH) bool {
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
		log.Printf("FATAL ERROR OV_H %d) Park mapdata.hid=%d mapdata.open=%t mapdata.preopen=%t hash='%s'", hid, mapdata.hid, mapdata.open, mapdata.preopen, ovfh.Hash)
		OV_handler.KILL(hid)
	}
	oh.mux.Unlock()

	return retval
} // end func OV_Handler.Park

func (oh *OV_Handler) Check_idle() {
	// oscilate between 25-250ms to check if we have to flush/close overviews
	isleep_min, isleep_max, isleep := 25, 250, 250

forever:
	for {
		start := utils.Nano()

		oh.mux.Lock()
		len_cv := len(oh.V)
		oh.mux.Unlock()

		if len_cv < OV_handler.MAX_OPEN_MMAPS/2 {
			isleep += 1
		} else {
			isleep -= 2
		}
		if isleep < isleep_min {
			isleep = isleep_min
		} else if isleep > isleep_max {
			isleep = isleep_max
		}

		need_stop := is_closed_server(oh.STOP)
		if need_stop {
			if len_cv == 0 {
				log.Printf("OV Check_idle len_cv=0 need_stop=%t", need_stop)
				break forever
			}
			time.Sleep(1 * time.Millisecond)
		} else {
			time.Sleep(time.Duration(isleep) * time.Millisecond)
		}

		// find one we can flush and close
		send_close := false
		var newdata OV_Handler_data
		var close_request Overview_Close_Request
		looped := 0

		oh.mux.Lock()

	find_one:
		for hash, data := range oh.V {
			looped++

			if hash != data.ovfh.Hash {
				if data.ovfh.Hash == "" { // not yet set?
					if oh.Debug {
						log.Printf("WARN OV Check_idle: data.ovfh.Hash is empty, not yet set? hash='%s'", hash)
					}
					continue find_one
				} else {
					log.Printf("FATAL ERROR OV Check_idle hash='%s' != data.ovfh.Hash='%s'", hash, data.ovfh.Hash)
					OV_handler.KILL(0)
					oh.mux.Unlock()
					break forever
				}
			}

			var lastflush int64
			if data.ovfh.Time_flush > 0 {
				lastflush = utils.Now() - data.ovfh.Time_flush
			}

			if data.hid == 0 && (need_stop || lastflush >= MAX_FLUSH) {
				if open_mmap_overviews.closelockMMAP(0, hash) {
					close_request.force_close = true
					close_request.ovfh = data.ovfh
					close_request.reply_chan = nil
					newdata = data
					newdata.hid = -2
					send_close = true
					time_open := utils.Now() - data.ovfh.Time_open
					written := data.ovfh.Written
					if oh.Debug || lastflush > MAX_FLUSH {
						log.Printf("OV Check_Idle: close  hash='%s' lastflush=%d open=%d written=%d len_cv=%d-1", hash, lastflush, time_open, written, len_cv)
					}
					break find_one
				}
			}

		} // end for find_one

		if send_close {
			oh.V[newdata.ovfh.Hash] = newdata
		}
		oh.mux.Unlock()

		if send_close {
			close_request_chan <- close_request
		}

		if oh.Debug {
			log.Printf("OV_Handler.Check_idle_Loop looped=%d took=%d ns", looped, utils.Nano()-start)
		}
	} // end for forever

} // end func OV_Handler.Check_idle_Loop

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
			if OV_handler.process_open_request(hid, open_request) {
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
	var closed uint64
	var close_errors uint64

for_closer:
	for {
		select {
		case close_request, ok := <-close_request_chan:
			if !ok {
				log.Printf("OV CLOSER %d) close_request_chan closed", hid)
				break for_closer
			}
			// got close_request for overview file ( Overview_Close_Request = { ovfh, reply_chan } )
			if OV_handler.process_close_request(hid, close_request) {
				closed++
			} else {
				log.Printf("OV_handler.process_close_request returned false")
				close_errors++
			}
		}
	}

	if close_errors > 0 {
		log.Printf("ERROR OV CLOSER %d) returned closed=%d close_errors=%d", hid, closed, close_errors)
	}
} // end func Overview_handler_CLOSER

/*
func (oh *OV_Handler) open_overviews() int {
	return len(count_open_overviews)
}
*/

func (oh *OV_Handler) process_close_request(hid int, close_request Overview_Close_Request) bool {
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
		log.Printf("(CLOSER) OV_H %d) close_request force_close=%t fp='%s'", hid, force_close, file)
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
			if ovfh, err := Update_Footer(close_request.ovfh); err != nil {
				log.Printf("FATAL ERROR (CLOSER) OV_H %d) process_close_request Update_Footer failed err='%v' fp='%s", hid, err, file)
				reply.err = err
				OV_handler.KILL(hid)
			} else {
				if oh.Debug {
					log.Printf("(CLOSER) OV_H %d) process_close_request flushed lastflush=%d fp='%s", hid, lastflush, file)
				}
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

			} // end Flush_ov
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
			<-count_open_overviews // suck one out reduces len of channel
			retval = true
			reply.retbool = retval
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

func (oh *OV_Handler) process_open_request(hid int, open_request Overview_Open_Request) bool {
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
		if ovfh, retbool := OV_handler.GetOpen(hid, open_request.file_path, open_request.hash); retbool == true {
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

func (oh *OV_Handler) KILL(hid int) {
	log.Printf("ERROR OV_H %d) KILL", hid)
	oh.STOP <- true
}
