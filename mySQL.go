package overview

import (
	"fmt"
	"log"
	"os"
	"time"
	"math/rand"
	"strings"
	"database/sql"
	"github.com/go-sql-driver/mysql"
)

func PrintHashMySQL(printrocksdb bool) {
	fmt.Println("# mySQL setup to fill database msgidhash  with tables h_0__ - h_f__")
	if !printrocksdb {
		fmt.Println("CREATE DATABASE IF NOT EXISTS `msgidhash` DEFAULT CHARACTER SET latin1 COLLATE latin1_general_ci;")
	} else {
		fmt.Println("CREATE DATABASE IF NOT EXISTS `msgidhash` DEFAULT CHARACTER SET latin1 COLLATE latin1_bin;")
	}
	fmt.Println("USE `msgidhash`;")
	cs := "0123456789abcdef"
	idx := 3 // NOTE: to change tables to use idx 1-3 search overview for "printhashsql" and edit all "0:2" and "2:" to 3!
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
	//query := "INSERT INTO h_"+string(messageidhash[0:2])+" (hash, fsize) VALUES (?,?)"
	stmt, err := db.Prepare("INSERT INTO h_"+string(messageidhash[0:3])+" (hash, fsize) VALUES (?,?)"); // printhashsql cut first N chars
	if err != nil {
		log.Printf("ERROR overview.MsgIDhash2mysql db.Prepare() err='%v'", err)
		return false, err
	}
	defer stmt.Close()
	if res, err := stmt.Exec(messageidhash[3:], size); err != nil { // printhashsql cut first N chars
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
	if err := db.QueryRow("SELECT stat FROM "+"h_"+string(messageidhash[0:3])+" WHERE hash = ? LIMIT 1", messageidhash[3:]).Scan(&stat); err != nil { // printhashsql cut first N chars
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
	/*
	if hash == messageidhash[3:] { // printhashsql cut first N chars
		var drop bool
		if stat.Valid {
			drop = true
		}

		switch stat.String {
			case "a":
				// abuse takedown
				drop = true
			case "b":
				// banned
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
			case "n":
				// nocem
				drop = true
			case "p":
				// filtered by pyClean
				drop = true
			case "r":
				// removed
				drop = true
			case "s":
				// filtered by spam assasin
				drop = true
		}

		return true, drop, stat.String, nil
	}
	return false, false, "", fmt.Errorf("ERROR overview.IsMsgidHashSQL() uncatched return")
	*/
} // end func IsMsgidHashSQL