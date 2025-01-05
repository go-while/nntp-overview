package overview


import (
	"fmt"
	"log"
	"os"
	"time"
	"math/rand"
	"strings"
	"sync"
	"database/sql"
	"github.com/go-while/go-utils"
	"github.com/go-sql-driver/mysql"
)

var (
	Psql = 256 // parallel sql threads
	Flushmax = 4096 // * 16^idx = max cached to flush
	idx = 3 // hardcoded
	cs = "0123456789abcdef"
)

func PrintHashMySQL(printrocksdb bool) {
	fmt.Println("# mySQL setup to fill database msgidhash  with tables h_0__ - h_f__")
	if !printrocksdb {
		fmt.Println("CREATE DATABASE IF NOT EXISTS `msgidhash` DEFAULT CHARACTER SET latin1 COLLATE latin1_general_ci;")
	} else {
		fmt.Println("CREATE DATABASE IF NOT EXISTS `msgidhash` DEFAULT CHARACTER SET latin1 COLLATE latin1_bin;")
	}
	fmt.Println("USE `msgidhash`;")

	 // NOTE: to change tables to use idx 1-3 search overview for "printhashsql" and edit all "0:2" and "2:" to 3!
	for _, c1 := range cs {
		if idx == 1 {
			if !printrocksdb {
				innodb := fmt.Sprintf("CREATE TABLE `h_%s` (  `hash` char(63) NOT NULL,  `fsize` int(11) DEFAULT NULL,  `stat` char(1) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;", string(c1))
				fmt.Println(innodb)
			} else {
				rocksd := fmt.Sprintf("CREATE TABLE `h_%s` (  `hash` char(63) NOT NULL,  `fsize` int(11) DEFAULT NULL,  `stat` char(1) DEFAULT NULL) ENGINE=RocksDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;", string(c1))
				fmt.Println(rocksd)
			}
			alters := fmt.Sprintf("ALTER TABLE `h_%s`  ADD PRIMARY KEY (`hash`);", string(c1))
			fmt.Println(alters)
			continue
		}
		for _, c2 := range cs {
			if idx == 2 {
				if !printrocksdb {
					innodb := fmt.Sprintf("CREATE TABLE `h_%s%s` (  `hash` char(62) NOT NULL,  `fsize` int(11) DEFAULT NULL,  `stat` char(1) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;", string(c1), string(c2))
					fmt.Println(innodb)
				} else {
					rocksd := fmt.Sprintf("CREATE TABLE `h_%s%s` (  `hash` char(62) NOT NULL,  `fsize` int(11) DEFAULT NULL,  `stat` char(1) DEFAULT NULL) ENGINE=RocksDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;", string(c1), string(c2))
					fmt.Println(rocksd)
				}
				alters := fmt.Sprintf("ALTER TABLE `h_%s%s`  ADD PRIMARY KEY (`hash`);", string(c1), string(c2))
				fmt.Println(alters)
				continue
			}
			for _, c3 := range cs {
				if idx == 3 {
					if !printrocksdb {
						innodb := fmt.Sprintf("CREATE TABLE `h_%s%s%s` (  `hash` char(61) NOT NULL,  `fsize` int(11) DEFAULT NULL,  `stat` char(1) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;", string(c1), string(c2), string(c3))
						fmt.Println(innodb)
					} else {
						rocksd := fmt.Sprintf("CREATE TABLE `h_%s%s%s` (  `hash` char(61) NOT NULL,  `fsize` int(11) DEFAULT NULL,  `stat` char(1) DEFAULT NULL) ENGINE=RocksDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;", string(c1), string(c2), string(c3))
						fmt.Println(rocksd)
					}
					alters := fmt.Sprintf("ALTER TABLE `h_%s%s%s`  ADD PRIMARY KEY (`hash`);", string(c1), string(c2), string(c3))
					fmt.Println(alters)
				}
			}
		}
	}
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

func ShortMsgIDhash2mysql(shorthash string, offset int, db *sql.DB) (bool, error) {
	return false, nil
} // end func ShortMsgIDhash2mysql

func MsgIDhash2mysql(messageidhash string, size int, db *sql.DB) (bool, error) {
	if len(messageidhash) != 64 || size == 0 {
		return false, fmt.Errorf("ERROR overview.MsgIDhash2mysql len(messageidhash)=%d != 64 || size=%d", len(messageidhash), size)
	}

	//tablename := "h_"+string(messageidhash[0:2])
	stmt, err := db.Prepare("INSERT INTO h_"+string(messageidhash[0:idx])+" (hash, fsize) VALUES (?,?)"); // printhashsql cut first N chars
	if err != nil {
		log.Printf("ERROR overview.MsgIDhash2mysql db.Prepare() err='%v'", err)
		return false, err
	}
	defer stmt.Close()
	if res, err := stmt.Exec(messageidhash[idx:], size); err != nil { // printhashsql cut first N chars
		//log.Printf("ERROR overview.MsgIDhash2mysql stmt.Exec() err='%v'", err)
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
	return false, fmt.Errorf("ERROR overview.MsgIDhash2mysql() uncatched return")
} // end func MsgIDhash2mysql

func MsgIDhash2mysqlStat(messageidhash string, stat string, db *sql.DB) (bool, error) {
	if len(stat) != 1 {
		return false, fmt.Errorf("ERROR overview.MsgIDhash2mysqlStat len(stat)=%d != 1", len(stat))
	}

	//tablename := "h_"+string(messageidhash[0:2])
	//query := "INSERT INTO h_"+string(messageidhash[0:2])+" (hash, fsize) VALUES (?,?)"
	stmt, err := db.Prepare("INSERT INTO h_"+string(messageidhash[0:idx])+" (hash, stat) VALUES (?,?) ON DUPLICATE KEY UPDATE stat = ?"); // printhashsql cut first N chars
	if err != nil {
		log.Printf("ERROR overview.MsgIDhash2mysqlStat db.Prepare() err='%v'", err)
		return false, err
	}
	defer stmt.Close()
	if res, err := stmt.Exec(messageidhash[3:], stat, stat); err != nil { // printhashsql cut first N chars
		//log.Printf("ERROR overview.MsgIDhash2mysqlStat stmt.Exec() err='%v'", err)
		return false, err
	} else {
		if rowCnt, err := res.RowsAffected(); err != nil {
			log.Printf("ERROR overview.MsgIDhash2mysqlStat res.RowsAffected() err='%v'", err)
			return false, err
		} else {
			if rowCnt == 1 {
				return true, nil // inserted
			}
			return false, nil // duplicate
		}
	}
	return false, fmt.Errorf("ERROR overview.MsgIDhash2mysqlStat() uncatched return")
} // end func MsgIDhash2mysqlStat

func MsgIDhash2mysqlMany(key string, list []Msgidhash_item, db *sql.DB, tried int) (bool, error) {
	if len(list) == 0 {
		return false, fmt.Errorf("ERROR overview.MsgIDhash2mysqlMany key=%s list empty", key)
	}
	if tried > 15 {
		log.Printf("ERROR MsgIDhash2mysqlMany tried=%d")
		os.Exit(1)
	}
	var vals []interface{}

	query := "INSERT IGNORE INTO h_"+key+" (hash, fsize) VALUES "
	for i, item := range list {
		if len(item.Hash) != 64 || item.Size <= 0 { // printhashsql
			log.Printf("ERROR overview.MsgIDhash2mysqlMany item='%#v' len(list)=%d i=%d key=%s", item, len(list), i , key)
			os.Exit(1)
		}
		query += "(?,?),"
		vals = append(vals, string(item.Hash[3:]), item.Size) // printhashsql cut first N chars
	}
	query = strings.TrimSuffix(query, ",")
	stmt, err := db.Prepare(query);
	if err != nil {
		log.Printf("ERROR overview.MsgIDhash2mysqlMany db.Prepare() key=%s err='%v'", key, err)
		return false, err
	}
	defer stmt.Close()
	if _, sqlerr := stmt.Exec(vals...); sqlerr != nil {
		if driverErr, isErr := sqlerr.(*mysql.MySQLError); isErr {
			retry := false
			switch driverErr.Number {
				case 1205:
					// Lock wait timeout exceeded; try restarting transaction
					retry = true
				case 1213:
					// Deadlock found when trying to get lock; try restarting transaction
					retry = true
			}
			if retry {
				isleep := time.Duration(rand.Intn(60))*time.Second
				if isleep < 5 {
					isleep = 5
				}
				time.Sleep(isleep)
				tried++
				return MsgIDhash2mysqlMany(key, list, db, tried)
			}
			log.Printf("overview.MsgIDhash2mysqlMany driverErr='%v' num=%d sqlerr='%v'", driverErr, driverErr.Number, sqlerr)
		}
		return false, err
	}
	//log.Printf("OK MsgIDhash2mysqlMany key=%s list=%d", key, len(list))
	return true, nil
} // end func MsgIDhash2mysqlMany

func IsMsgidHashSQL(messageidhash string, db *sql.DB) (bool, bool, string, error) {

	if len(messageidhash) != 64 { // printhashsql
		return false, false, "", fmt.Errorf("ERROR overview.IsMsgidHashSQL len(messageidhash) != 64")
	}
	var stat sql.NullString
	if err := db.QueryRow("SELECT stat FROM "+"h_"+string(messageidhash[0:idx])+" WHERE hash = ? LIMIT 1", messageidhash[idx:]).Scan(&stat); err != nil { // printhashsql cut first N chars
		if err == sql.ErrNoRows {
			return false, false, "", nil
		}
		log.Printf("ERROR overview.IsMsgidHashSQL err='%v'", err)
		return false, false, "", err
	}
	var drop bool
	if stat.Valid && len(stat.String) == 1 {
		drop = true
	}
	return true, drop, stat.String, nil
} // end func IsMsgidHashSQL

func ClearStat(stat string, db *sql.DB) (error) {
	/*
	 * SELECT *  FROM `h_00a` WHERE
	 * 		`stat` is not NULL AND
	 * 		`stat` != 'b' AND
	 * 		`stat` != 'z' AND
	 * 		`stat` != 'x' AND
	 * 		`stat` != 'o' AND
	 * 		`stat` != 'h'
	 * 		ORDER BY `stat` DESC;
	*/
	deleted := 0
	start := utils.UnixTimeMilliSec()
	for _, c1 := range cs {
		for _, c2 := range cs {
			for _, c3 := range cs {
				table := string(c1)+string(c2)+string(c3)
				query := "DELETE FROM "+"h_"+table+" WHERE fsize is NULL and stat = "+stat
				res, err := db.Exec(query)
				if err != nil {
					log.Printf("ERROR overview.ClearStat stat='%s' err='%v'", stat, err)
					return err
				}
				rowCnt, err := res.RowsAffected()
				if err != nil {
					log.Printf("ERROR overview.ClearStat stat='%s' res.RowsAffected() err='%v'", stat, err)
					return err
				}
				log.Printf("ClearStat stat='%s' table='h_%s%s%s' deleted=%d", stat, table, rowCnt)
				deleted++
			}
		}
	}
	took := utils.UnixTimeMilliSec() - start
	log.Printf("ClearStat stat='%s' deleted=%d took=(%d ms)", stat, deleted, took)
	return nil
} // end func IsMsgidHashSQL

/*
func IsMsgidSQL(messageid string, db *sql.DB) (bool, bool, string, error) {

	if len(messageid) <= 0 { // printhashsql
		return false, false, "", fmt.Errorf("ERROR overview.IsMsgidSQL len(messageid) <= 0")
	}
	var stat sql.NullString
	if err := db.QueryRow("SELECT stat FROM xxxxxxxxxxxxxxxxxxxxx`CONCAT('h_',SUBSTRING(SHA2(?,256),1,3))` WHERE hash = SHA2(?,256) LIMIT 1", messageid, messageid).Scan(&stat); err != nil { // printhashsql cut first N chars
		if err == sql.ErrNoRows {
			return false, false, "", nil
		}
		log.Printf("ERROR overview.IsMsgidSQL err='%v'", err)
		return false, false, "", err
	}
	var drop bool
	if stat.Valid && len(stat.String) == 1 {
		drop = true
	}
	return true, drop, stat.String, nil
} // end func IsMsgidHashSQL
*/

	/*
	switch stat.String {
		case "a":
			// abuse takedown
			drop = true
		case "b":
			// banned / no accepted newsgroups
			drop = true
		case "c":
			// cancel
			drop = true
		case "d":
			// to delete
			drop = true
		case "e":
			// expired
			drop = true
		case "f":
			// filtered by cleanfeed
			drop = true
		case "g":
			// group removed
			drop = true
		case "h":
			// bad/nil head/body
			drop = true
		case "n":
			// nocem
			drop = true
		case "o":
			// bad header overview checksum
			drop = true
		case "p":
			// filtered by pyClean
			drop = true
		case "r":
			// removed by overview spamfilter
			drop = true
		case "s":
			// filtered by spam assasin
			drop = true
		case "x":
			// crosspost
			drop = true
		case "z":
			// prefetch/proxy/binary filter
			drop = true
	}
	*/

func ProcessHash2sql(dbh *sql.DB, hash2sql *chan map[string][]Msgidhash_item, donechan *chan struct{}, sqldonechan *chan struct{}, sqlparchan *chan struct{}, wg *sync.WaitGroup) {
	defer dbh.Close()
	wg.Add(1)
	defer wg.Done()
	done := false

	waited, dupes, flushed := 0, 0, 0
	tmpmap := make(map[string]map[string]Msgidhash_item, Flushmax) // key, msgidhash, size
	start := utils.UnixTimeSec()
	process_hash2sql:
	for {
		select {
			case <- *donechan:
				done = true
			case hashmap := <- *hash2sql:
				waited = 0
				for key, items := range hashmap {
					tryflush1 := len(tmpmap[key]) >= Flushmax/4
					tryflush2 := len(*sqlparchan) > Psql/2
					tryrand := rand.Intn(1000) <= 20 // flush 2% early?
					flushnow := (tryflush1 && tryrand) || (tryflush1 && tryflush2)
					forceflush := len(tmpmap[key]) >= Flushmax
					if flushnow || forceflush {
						//if flushnow {
						//	log.Printf("process_hash2sql flushnow=%d", len(tmpmap[key]))
						//}
						// tmpmap for key is big! push to sql first
						var list []Msgidhash_item
						for _, item := range tmpmap[key] {
							if len(item.Hash) == 64 && item.Size > 0 {
								list = append(list, item)
							} else {
								log.Printf("WARN process_hash2sql got nil-hash item?")
							}
						}
						<- *sqlparchan
						wg.Add(1)
						go func(key string, list []Msgidhash_item, dbh *sql.DB, sqlparchan *chan struct{}) {
							//log.Printf("hash2sql key=%s flushing=%d list=%d", key, len(tmpmap[key]), len(list))
							//startms := utils.UnixTimeMilliSec()
							if retbool, sqlerr := MsgIDhash2mysqlMany(key, list, dbh, 0); sqlerr != nil || !retbool {
								log.Printf("ERROR process_hash2sql tmpmap sqlerr='%v'", sqlerr)
								os.Exit(1)
							}
							//log.Printf("hash2sql flushed=%d key=%s took=(%d ms)", len(list), key, utils.UnixTimeMilliSec()-startms)
							*sqlparchan <- struct{}{}
							wg.Done()
						}(key, list, dbh, sqlparchan)

						flushed += len(list)
						// reset tmpmap
						tmpmap[key] = make(map[string]Msgidhash_item, Flushmax)
						list = nil
					}
					// merge items / dedupe up to Flushmax
					for _, item := range items {
						if tmpmap[key] == nil {
							tmpmap[key] = make(map[string]Msgidhash_item, Flushmax)
						}
						if tmpmap[key][item.Hash].Size > 0 {
							dupes++
							continue
						}
						tmpmap[key][item.Hash] = item
					} // end for item in items
				} // end for items in hashmap
				hashmap = nil

			default:
				time.Sleep(10*time.Millisecond)
				if done {
					waited++
					if waited >= 6000 { // 60s
						var keys []string
						for key, _ := range tmpmap {
							keys = append(keys, key)
						}
						for _, key := range keys {
							var list []Msgidhash_item
							for _, item := range tmpmap[key] {
								list = append(list, item)
							}
							wg.Add(1)
							go func(key string, list []Msgidhash_item, dbh *sql.DB, sqlparchan *chan struct{}) {
								<- *sqlparchan
								//log.Printf("hash2sql key=%s flushing=%d list=%d", key, len(tmpmap[key]), len(list))
								if retbool, sqlerr := MsgIDhash2mysqlMany(key, list, dbh, 0); sqlerr != nil || !retbool {
									log.Printf("ERROR process_hash2sql tmpmap sqlerr='%v'", sqlerr)
									os.Exit(1)
								}
								//log.Printf("hash2sql flushed=%d key=%s took=(%d ms)", len(list), key, utils.UnixTimeMilliSec()-startms)
								*sqlparchan <- struct{}{}
								wg.Done()
							}(key, list, dbh, sqlparchan)
							flushed += len(list)
							list = nil
						}
						break process_hash2sql
					}
				}
		} // end select
	} // end for process_hash2sql
	*sqldonechan <- struct{}{}
	log.Printf("process_hash2sql returned flushed=%d dupes=%d rt=(%d s)", flushed, dupes, utils.UnixTimeSec()-start)
} // end ProcessHash2sql
