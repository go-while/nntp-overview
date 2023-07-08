package overview

import (
    "log"
    "sync"
)

type Known_MessageIDs struct {
    v               map[string]bool     // map for quick access
    l               []string            // holds an ordered list of messageids
    Debug           bool                // print debug messages
    MAP_MSGIDS      int                 // capacity
    mux sync.Mutex
}

func (c *Known_MessageIDs) SetKnown(msgid string) bool {
    /*
     *  usage:
     *      if !overview.Known_msgids.SetKnown(hash) {
     *          return "435 Duplicate"
     *      }
     *
     */
    retval := false
    c.mux.Lock()
    if len(c.v) == c.MAP_MSGIDS { // map is full, drop one from store in slice 'l'
        clear_msgid := c.l[0] // fetch oldest entry
        delete(c.v, clear_msgid)
        c.l = c.l[1:] // pop and shift slice without first (oldest) entry
    }
    if !c.v[msgid] {
        c.v[msgid] = true
        c.l = append(c.l, msgid)
        retval = true
    }
    c.mux.Unlock()
    if c.Debug { log.Printf("SetKnown msgid=%s retval=%t", msgid, retval) }
    return retval
}

func (c *Known_MessageIDs) UnsetKnown(msgid string) {
    c.mux.Lock()
    delete(c.v, msgid)
    c.mux.Unlock()
    if c.Debug { log.Printf("unsetKnown msgid=%s", msgid) }
}
