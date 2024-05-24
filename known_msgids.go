package overview

import (
	"log"
	"sync"
)

type Known_MessageIDs struct {
	v          map[string]bool // key: messageidhash, val: true (or false if unknown/not set)
	l          []string        // holds an ordered list of msgidhashs
	Debug      bool            // print debug messages
	MAP_MSGIDS int             // capacity
	mux        sync.Mutex
}

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
	retval := false
	km.mux.Lock()
	if !km.v[msgidhash] { // msgidhash is false == not in map
		if len(km.v) == km.MAP_MSGIDS { // map is full, drop one from store in slice 'l'
			for {
				if len(km.l) == 0 {
					break
				}
				clear_msgid := km.l[0] // fetch oldest entry
				km.l = km.l[1:]        // and shift slice
				if km.v[clear_msgid] {
					delete(km.v, clear_msgid) // delete oldest from map
					break
				}
			}
		}
		km.v[msgidhash] = true         // adds new msgidhash to map
		km.l = append(km.l, msgidhash) // appends to slice
		retval = true
	}
	km.mux.Unlock()

	if km.Debug {
		log.Printf("SetKnown msgidhash=%s retval=%t", msgidhash, retval)
	}
	return retval
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
	var newl []string
	for _, v := range km.l {
		if v != msgidhash {
			newl = append(newl, v)
		}
	}
	km.l = newl
	km.mux.Unlock()

	if km.Debug {
		log.Printf("unsetKnown msgidhash=%s", msgidhash)
	}
} // end func UnsetKnown
