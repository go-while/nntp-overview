package overview

import (
	"log"
	"sync"
	"time"
)

type Known_MessageIDs struct {
	v          map[string]int64 // key: messageidhash, val: true (or false if unknown/not set)
	Debug      bool            // print debug messages
	MAP_MSGIDS int             // capacity
	mux        sync.Mutex
}


func (km *Known_MessageIDs) ExpireThread() {
	go func(){
		log.Print("Known_msgids.ExpireThread() start")
		for {
			deleted := 0
			time.Sleep(time.Second*5)
			km.mux.Lock()
			for msgidhash, expires := range km.v {
				if expires > time.Now().Unix() {
					go km.UnsetKnown(msgidhash)
					deleted++
				}
			}
			km.mux.Unlock()
			if deleted > 0 {
				log.Printf("KnownMsgIds ExpireThread deleted=%d", deleted)
			}
		}
	}()
} // end func ExpireThread

func (km *Known_MessageIDs) SetKnown(msgidhash string) bool {
	/*
	 *  SetKnown:
	 *      : call when receiving a messageid via commands: (IHAVE, TAKETHIS, POST, CHECK)
	 *      + func returns true if msgidhash was not known
	 *      - func returns false if msgidhash is known
	 *  usage:
	 *      if !overview.Known_msgids.SetKnown(msgidhash) {
	 *          return "435 Duplicate"
	 *      }
	 *      // else: reply OK to command, and receive data
	 *
	 */
	km.mux.Lock()
	defer km.mux.Unlock()

	if km.v == nil {
		log.Printf("ERROR OVERVIEW SetKnown km.v nil")
		return false
	}

	if km.v[msgidhash] >= time.Now().Unix() {
		if km.Debug {
			log.Printf("NOT SetKnown msgidhash=%s expires in='%d sec'", msgidhash, km.v[msgidhash]-time.Now().Unix())
		}
		return false
	}

	km.v[msgidhash] = time.Now().Unix()+15         // adds new msgidhash to map

	if km.Debug {
		log.Printf("SetKnown msgidhash=%s", msgidhash)
	}
	return true
} // end func SetKnown

func (km *Known_MessageIDs) UnsetKnownQuick(msgidhash string) {
	km.mux.Lock()
	delete(km.v, msgidhash)
	km.mux.Unlock()
} // end func UnsetKnownQuick

func (km *Known_MessageIDs) UnsetKnown(msgidhash string) {
	/*
	 *  NOTE: calling UnsetKnown() is expensive
	 *         SetKnown clears up map when full
	 *
	 *  usage:
	 *      overview.Known_msgids.UnsetKnown(msgidhash)
	 *
	 *
	 */
	km.mux.Lock()
	delete(km.v, msgidhash)
	km.mux.Unlock()

	if km.Debug {
		log.Printf("unsetKnown msgidhash=%s", msgidhash)
	}
} // end func UnsetKnown
