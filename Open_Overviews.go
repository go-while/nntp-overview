package overview

import (
	"github.com/go-while/go-utils"
	"log"
	"sync"
)

// every worker checks vs Open_MMAP_Overviews if any other worker is processing this group right now
// if map[group]bool returns true, waiting worker places a channel in ch and waits for return signal
type Open_MMAP_Overviews struct {
	v   map[string]int64           // key: grouphash, val: unixtimesec as locktime
	ch  map[string][]chan struct{} // key: grouphash: val: slice of chans for signal_channels
	mux sync.Mutex
}

func (omo *Open_MMAP_Overviews) isopenMMAP(hash string) bool {
	retval := false
	omo.mux.Lock()

	if omo.v[hash] > 0 {
		retval = true
	}

	omo.mux.Unlock()
	if DEBUG_OV {
		log.Printf("isopenMMAP hash='%s' retval=%t", hash, retval)
	}
	return retval
} // end func isopenMMAP

func (omo *Open_MMAP_Overviews) closelockMMAP(hid int, hash string) bool {
	retval := false
	omo.mux.Lock()

	locktime := omo.v[hash]
	if locktime == 0 {
		omo.v[hash] = utils.Nano()
		retval = true
	}

	omo.mux.Unlock()
	return retval
} // end func closelockMMAP

func (omo *Open_MMAP_Overviews) lockMMAP(hid int, hash string, signal_chan chan struct{}) (bool, chan struct{}) {
	omo.mux.Lock()

	len_cv := len(omo.v)
	locktime := omo.v[hash]
	if locktime > 0 {
		omo.ch[hash] = append(omo.ch[hash], signal_chan)
		omo.mux.Unlock()
		if DEBUG_OV {
			log.Printf("!!!! (OPENER) OV_H %d) lockMMAP islocked locktime=%d -> placed signal_chan @pos=%d hash='%s'", hid, locktime, len(omo.ch[hash]), hash)
		}
		return false, signal_chan
	}

	omo.v[hash] = utils.Nano()
	omo.mux.Unlock()
	if DEBUG_OV {
		log.Printf("#### (OPENER) OV_H %d) lockMMAP hash='%s' true len_cv=%d ", hid, hash, len_cv)
	}
	return true, nil
} // end func lockMMAP

func (omo *Open_MMAP_Overviews) unlockMMAP(hid int, force_close bool, hash string) {
	if DEBUG_OV {
		log.Printf("<--- OV_H %d) unlockMMAP hash='%s'", hid, hash)
	}
	var signal_chan chan struct{} = nil
	omo.mux.Lock()

	locktime := omo.v[hash]
	if locktime == 0 {
		omo.mux.Unlock()
		return
	}

	waiting_worker := len(omo.ch[hash])
	if waiting_worker > 0 {
		if DEBUG_OV {
			log.Printf("~~~~ (CLOSER) OV_H %d) unlockMMAP waiting_worker=%d locktime=%d hash='%s' ", hid, waiting_worker, locktime, hash)
		}
		signal_chan, omo.ch[hash] = omo.ch[hash][0], omo.ch[hash][1:] // popList
	} else {
		if DEBUG_OV {
			log.Printf("~~~~ (CLOSER) OV_H %d) unlockMMAP waiting_worker=0 clear map locktime=%d hash='%s' ", hid, locktime, hash)
		}
		delete(omo.v, hash)
	}
	omo.mux.Unlock()

	if signal_chan != nil {
		if DEBUG_OV {
			log.Printf("<--- (CLOSER) OV_H %d) unlockMMAP signal locktime=%d  hash='%s'", hid, locktime, hash)
		}
		signal_chan <- struct{}{}
	}
} // end func unlockMMAP
