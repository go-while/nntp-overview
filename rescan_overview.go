package overview

import (
	"database/sql"
	"fmt"
	//"github.com/edsrzf/mmap-go"
	"github.com/go-while/go-utils"
	"log"
	"os"
	"path/filepath"
	"strings"
	//"time"
	//"github.com/go-sql-driver/mysql"

)

type CHECK_GROUPS struct {
	group     string
	hash      string
	file_path string
}

func Rescan_help() {
	fmt.Println("--rescan-mode=help:")
	fmt.Println(" > OVERVIEW RESCAN options: no-fix, scan-only")
	fmt.Println("   mode: 0 == full rescan with verify fields")
	fmt.Println("   mode: 1 == check only header")
	fmt.Println("   mode: 2 == check only footer")
	fmt.Println("   mode: 3 == like mode 1 + 2 + count only lines and match msgnums==lines")
	fmt.Println("   mode: 4 == like mode 0 but checks footer after (not before) verify lines/fields")
	// FIXME TODO fmt.Println("   mode: 5 == like mode 0 but checks every msgid if head+body exists on disk")
	fmt.Println("   ")
	fmt.Println(" > FIX / REBUILD options")
	fmt.Println("   mode: 997 == like mode 3 with quíck rebuild ActiveMap")
	fmt.Println("   mode: 998 == like mode 0 with deep check and safer but slower rebuild ActiveMap")
	fmt.Println("   mode: 999 == like mode 4 with try fix-footer!")
	fmt.Println("   mode: 1000 == only insert messageidhash to mysql")
	os.Exit(0)
}

func returndefermmapclose(ovfh *OVFH, cancelchan chan struct{}) {
	select {
	case <-cancelchan:
		return
	default:
	}
	utils.MMAP_CLOSE(ovfh.File_path, ovfh.File_handle, ovfh.Mmap_handle, "rw")
}

type Msgidhash_item struct {
	Hash string
	Size int
}

// Rescan_Overview returns: true|false, last_msgnum
func Rescan_Overview(who string, file_path string, group string, mode int, DEBUG bool, db *sql.DB, hash2sql *chan map[string][]Msgidhash_item) (bool, uint64) {
	// the steps are:
	// check header
	// check footer ('check footer' runs before 'check lines' or at the end if footer is broken, use mode=4 setting)
	// check lines
	// verify fields
	// check footer

	if mode < 1000 {
		log.Printf(" -> Start Rescan_Overview: fp='%s' group='%s' mode=%d", file_path, group, mode)
	}
	//time.Sleep(time.Second)
	var err error
	var fix_flag string
	//var sql_ins int
	ovfh := &OVFH{}
	ovfh.File_path = file_path

	ovfh.File_handle, ovfh.Mmap_handle, err = utils.MMAP_FILE(file_path, "rw")
	if err != nil {
		log.Printf("ERROR Rescan_OV MMAP_FILE err='%v' e01", err)
		return false, 0
	}
	cancelchan := make(chan struct{}, 1)
	defer returndefermmapclose(ovfh, cancelchan)

	len_mmap := len(ovfh.Mmap_handle)

	if len_mmap < 1024+OV_RESERVE_BEG+OV_RESERVE_END {
		log.Printf("ERROR Rescan_OV len_mmap=%d <= 1024+OV_RESERVE_BEG+OV_RESERVE_END", len_mmap)
		if mode != 999 {
			log.Printf("CONSIDER: delete .overview file because is too small or empty?")
			return false, 0
		}
	}

	startindex := OV_RESERVE_BEG
	endindex := len_mmap - 1

	msgidhashmap := make(map[string][]Msgidhash_item)

	if mode < 1000 {
		log.Printf(" --> rescan_OV mode=%d len=%d startindex=%d", mode, len_mmap, startindex)
	} else {

	}

	if mode == 0 || mode == 1 || mode == 997 || mode == 998 {
		ov_header_line := string(ovfh.Mmap_handle[:OV_RESERVE_BEG])
		if retbool := check_ovfh_header(who, ov_header_line); retbool == false {
			log.Printf("ERROR Rescan_OV check_ovfh_header retbool=%t", retbool)
			return false, 0
		}
	}
	if mode == 1 {
		return true, 0
	}

	if mode == 0 || mode == 2 || mode == 997 || mode == 998 {
		ov_footer_line := string(ovfh.Mmap_handle[len_mmap-OV_RESERVE_END:])
		if retbool := check_ovfh_footer(who, ov_footer_line); retbool == false {
			log.Printf("ERROR Rescan_OV check_ovfh_footer retbool=%t footer='%s'", retbool, string(ov_footer_line))
			return false, 0
		}
	}
	if mode == 2 {
		return true, 0
	}

	line, lines, newlines, tabs, frees, position := "", 0, 0, 0, 0, OV_RESERVE_BEG
	var last_msgnum uint64
	last_line, last_newlines, last_tabs, last_beg, last_newline_pos := line, newlines, tabs, position, position
	uniq_msgids := make(map[string]int)
	var list_msgids []string
	badfooter := false

	if mode < 1000 {
		log.Printf("rescan_OV: startindex=%d endindex=%d", startindex, endindex)
	}

rescan_OV:
	for i := startindex; i <= endindex; i++ {
		c := ovfh.Mmap_handle[i] // byte char at index i

		switch c { // check what this byte char is
		case 0:
			frees++

		case '\t':
			tabs++
			line = line + string(c)

		case '\n':
			newlines++
			if DEBUG_OV {
				log.Printf(" --> Rescan_OV got newline@i=%d tabs=%d newlines=%d fp='%s'", i, tabs, newlines, filepath.Base(file_path))
			}

			// found frees: <nul> bytes
			if frees > 0 {
				if mode == 1000 {
					break rescan_OV
				}
				log.Printf(" --> Rescan_OV frees=%d@i=%d tabs=%d newlines=%d fp='%s'", frees, i, tabs, newlines, filepath.Base(file_path))
				if last_newline_pos+1 != i-frees {
					log.Printf(" --> ERROR last_newline_pos=%d != i=%d-frees=%d fp='%s'", last_newline_pos, i, frees, filepath.Base(file_path))
					return false, 0
				}
				from := i
				end := len_mmap - 1 - 3
				rem := string(ovfh.Mmap_handle[from : end-1]) // from to before the 'EOF'
				eof := string(ovfh.Mmap_handle[end-1:])       // should be '\nEOF\n'
				log.Printf(" ** from i=%d from=%d end=%d eof='%s' rem='%s'", i, from, end, eof, rem)
				// check rem string backwards for <nul>,
				if rem[len(rem)-2] == 0 && rem[len(rem)-1] == ',' && eof == FOOTER_END {
					// capture footer content
					if len(rem) < 5 {
						log.Printf("ERROR Rescan_OV ov_footer='%s' len(rem) < 5 fp='%s'", rem, filepath.Base(file_path))
						return false, 0
					}
					ov_footer := rem[5:]
					//ov_footer := string(rem[4:]) // 4: should remove 'EOV\n' from good footer string

					// verify footer
					if !strings.HasPrefix(rem[:5], "\nEOV\n") {
						log.Printf("ERROR Rescan_OV rem[:5]='%s' !prefix EOV\\n fp='%s'", rem[:5], filepath.Base(file_path))
						return false, 0
					}
					if !strings.HasPrefix(ov_footer, "time=") {
						log.Printf("ERROR Rescan_OV rem='%s' ov_footer='%s' fp='%s'", rem, ov_footer, filepath.Base(file_path))
						return false, 0
					}
					foot := strings.Split(ov_footer, ",")
					if len(foot) != SIZEOF_FOOT {
						log.Printf("ERROR Rescan_OV foot=%d != %d fp='%s'", len(foot), SIZEOF_FOOT, filepath.Base(file_path))
						return false, 0
					}

					foot_time := strings.Split(foot[0], "=")
					foot_last := strings.Split(foot[1], "=")
					foot_index := strings.Split(foot[2], "=")
					foot_bodyend := strings.Split(foot[3], "=")
					foot_fend := strings.Split(foot[4], "=")
					foot_zero := strings.Split(foot[5], "=")

					if foot_time[0] != "time" {
						log.Printf("ERROR Rescan_OV key: time not found fp='%s'", filepath.Base(file_path))
						return false, 0
					}

					if foot_last[0] != "last" {
						log.Printf("ERROR Rescan_OV key: last not found fp='%s'", filepath.Base(file_path))
						return false, 0
					}

					if foot_index[0] != "Findex" {
						log.Printf("ERROR Rescan_OV key: Findex not found fp='%s'", filepath.Base(file_path))
						return false, 0
					}

					if foot_bodyend[0] != "bodyend" {
						log.Printf("ERROR Rescan_OV key: bodyend not found fp='%s'", filepath.Base(file_path))
						return false, 0
					}

					if foot_fend[0] != "fend" {
						log.Printf("ERROR Rescan_OV key: fend not found fp='%s'", filepath.Base(file_path))
						return false, 0
					}

					if foot_zero[0] != "zeropad" {
						log.Printf("ERROR Rescan_OV key: zeropad not found fp='%s'", filepath.Base(file_path))
						return false, 0
					}

					// verify footer numbers
					f_time := utils.Str2int64(foot_time[1])
					f_last := utils.Str2uint64(foot_last[1])
					f_indx := utils.Str2int(foot_index[1])
					f_bodyend := utils.Str2int(foot_bodyend[1])
					f_fend := utils.Str2int(foot_fend[1])
					f_zero := len(foot_zero[1])

					if f_time == 0 {
						log.Printf("ERROR Rescan_OV footer time=0 fp='%s'", filepath.Base(file_path))
						return false, 0
					}
					last_modified := (utils.Nano() - f_time) / 1e9
					log.Printf(" --> Rescan_OV footer f_time=%d age=%d fp='%s'", f_time/1e9, last_modified, filepath.Base(file_path))

					if f_last == 0 || f_last-1 != last_msgnum {

						if mode != 999 {
							log.Printf("ERROR Rescan_OV footer last=%d != last_msgnum=%d fp='%s'", f_last, last_msgnum, filepath.Base(file_path))
							return false, 0
						}
						badfooter = true
						if mode == 999 {
							fix_flag = "fix-footer"
						}
						log.Printf("WARN Rescan_OV footer last=%d != last_msgnum=%d fp='%s' badfooter=True", f_last, last_msgnum, filepath.Base(file_path))

					} else {
						log.Printf(" --> Rescan_OV footer last=OK=%d  fp='%s'", f_last, filepath.Base(file_path))
					}

					if f_indx != last_newline_pos+1 {
						diff := f_indx - last_newline_pos
						log.Printf("ERROR Rescan_OV footer Findex=%d != last_newline_pos=%d+1 diff=%d fp='%s'", f_indx, last_newline_pos, diff, filepath.Base(file_path))
						return false, 0
					} else {
						log.Printf(" --> Rescan_OV footer Findex=OK=%d fp='%s'", f_indx, filepath.Base(file_path))
					}

					if f_fend-f_bodyend != OV_RESERVE_END {
						log.Printf("ERROR Rescan_OV footer f_fend=%d - f_bodyend=%d ==%d != OV_RESERVE_END=%d fp='%s'", f_fend, f_bodyend, f_fend-f_bodyend, OV_RESERVE_END, filepath.Base(file_path))
						return false, 0
					}
					if f_zero == 0 {
						log.Printf("ERROR Rescan_OV footer f_zero=%d fp='%s'", f_zero, filepath.Base(file_path))
						return false, 0
					} else {
						log.Printf(" --> Rescan_OV footer f_zero=%d fp='%s'", f_zero, filepath.Base(file_path))
					}

					log.Printf(" > END Rescan_OV ov_footer='%s' len=%d  tabs=%d newlines=%d last_newline_pos=%d pos=%d fp='%s' badfooter=%t", ov_footer, len(ov_footer), tabs, newlines, last_newline_pos, position, filepath.Base(file_path), badfooter)
					if !badfooter {
						/*
							if mode != 997 && mode != 998 {
								return true, last_msgnum
							} else {
								break rescan_OV
							}*/
						return true, last_msgnum
					}
				} else {
					log.Printf("ERROR Rescan_OV rem='%s' eof='%s' fp='%s'", rem, eof, filepath.Base(file_path))
					return false, 0
				}
			} // end if frees > 0

			if newlines != 1 {
				log.Printf("ERROR Rescan_OV#1 @line=%d newlines=%d tabs=%d startindex=%d position=%d", lines, newlines, tabs, startindex, position)
				return false, 0
			}
			last_newline_pos = position

			if position == OV_RESERVE_BEG && tabs == 0 {
				newlines = 0
				position++
				// count lines without 1st header-line
				continue rescan_OV
			}

			var fields []string
			var msgnum uint64

			if tabs == OVERVIEW_TABS {

				fields = strings.Split(line, "\t")
				len_fields := len(fields)
				if len_fields < OVERVIEW_FIELDS {
					log.Printf("ERROR Rescan_OV#3 @line=%d newlines=%d fields=%d<%d tabs=%d startindex=%d position=%d line='%s'", lines, newlines, len_fields, OVERVIEW_FIELDS, tabs, startindex, position, line)
					return false, 0
				}
				lines++ // raise overview line counter
				msgnum = utils.Str2uint64(fields[0])
				/*
					if msgnum != lines {
						log.Printf("ERROR Rescan_OV#4 @line=%d msgnum=%d", lines, msgnum)
						return false, 0
					}*/

				// be verbose for every line in overview
				if DEBUG_OV {
					log.Printf(" ----> Rescan: line=%d msgnum=%d tabs=%d newslines=%d len=%d pos=%d last_newline_pos=%d", lines, msgnum, tabs, newlines, len(line), position, last_newline_pos)
				}
				last_msgnum = msgnum
				//last_line = line
			} else {
				log.Printf("WARN Rescan_OV#2 @line=%d found newline but tabs=%d startindex=%d pos=%d i=%d frees=%d end=%d ovfh.Mmap_handle=%d line='%s'", lines, tabs, startindex, position, i, frees, len(ovfh.Mmap_handle)-1, len(ovfh.Mmap_handle), line)
				//return false
			}

			if mode == 999 {

				if i >= len(ovfh.Mmap_handle)-1 {
					last_line, last_newlines, last_tabs, last_beg = line, newlines, tabs, position-len(line) // capture
					position++
					log.Printf("break rescan_OV last_msgnum=%d i=%d end=%d toend=%d", last_msgnum, i, len(ovfh.Mmap_handle), len(ovfh.Mmap_handle)-i)
					badfooter = true
					fix_flag = "fix-footer"
					break rescan_OV
				}

			}

			if mode == 3 || mode == 997 {
				last_line, last_newlines, last_tabs, last_beg = line, newlines, tabs, position-len(line) // capture
				line, newlines, tabs = "", 0, 0                                                          // reset looped values and try to find next line
				position++
				continue rescan_OV
			}

			if tabs == OVERVIEW_TABS && mode >= 998 {
				// deep verify scan of fields

				/*	ovl.MsgNum       int64       0
				 *	ovl.Subject      string      1
				 *	ovl.From         string      2
				 *	ovl.Date         string      3
				 *	ovl.Messageid    string      4
				 *	references       []string    5
				 *	ovl.Bytes        int         6
				 *	ovl.Lines        int         7
				 *	ovl.Xref         string      8
				*/

				if mode == 1000 {
					// insert to mysql: full messageidhash with size in bytes
					//if db == nil {
					//	log.Printf("ERROR Rescan_OV mode=1000 mysql_db=nil")
					//	return false, 0
					//}
					messageidhash := utils.Hash256(fields[4])
					bytes := utils.Str2int(fields[6])
					if bytes == 0 {
						log.Printf("ERROR prepare MsgIDhash2mysql size=0 msgid='%s' hash='%s' msgnum=%s f6=%s fields=%d", fields[4], messageidhash, fields[0], fields[6], len(fields))
						for i, field := range fields {
							log.Printf("field %d = '%s'", i, field)
						}
						return false, 0
					}
					key := string(messageidhash[0:3]) // printhashsql cut first N chars
					var item Msgidhash_item
					item.Hash = messageidhash
					item.Size = bytes
					msgidhashmap[key] = append(msgidhashmap[key], item)
					// insert sql
					last_line, last_newlines, last_tabs, last_beg = line, newlines, tabs, position-len(line) // capture
					line, newlines, tabs = "", 0, 0                                                          // reset looped values and try to find next line
					position++
					continue rescan_OV
				}

				if mode == 1001 {
					// insert to mysql: shorted messageidhash with offsets into history file
					if db == nil {
						log.Printf("ERROR Rescan_OV mode=1001 mysql_db=nil")
						return false, 0
					}
				}

				msgid := fields[4]
				//references := fields[5]
				bytes := utils.Str2int(fields[6])
				deezlines := utils.Str2int(fields[7])
				full_xref_str := fields[8]

				// start verify fields
				if !isvalidmsgid(msgid, false) {
					log.Printf("ERROR Rescan_OV#5 @line=%d !isvalidmsgid msgnum=%d", lines, msgnum)
					return false, 0
				}
				if uniq_msgids[msgid] > 0 {
					//log.Printf("WARN Rescan_OV#5a @line=%d !uniq_msgid msgnum=%d msgid='%s' firstL=%d", lines, msgnum, msgid, uniq_msgids[msgid])
					//return false, last_msgnum

				} else {
					uniq_msgids[msgid] = lines // is uniq at line N
					list_msgids = append(list_msgids, msgid)
				}

				if bytes <= 0 || deezlines <= 0 {
					log.Printf("ERROR Rescan_OV#6 @line=%d msgnum=%d bytes=%d lines=%d", lines, msgnum, bytes, deezlines)
					return false, 0
				}

				// check xrefs
				xrefs := strings.Split(full_xref_str, " ")
				// first xref has to be nntp, then group:n
				if len(xrefs) >= 2 && xrefs[0] == "nntp" {
					// loop over all xrefs we have
					for x := 1; x < len(xrefs); x++ {

						axref := xrefs[x]
						xrefdata := strings.Split(axref, ":")
						len_xrefdata := len(xrefdata)

						if len_xrefdata != 2 {
							log.Printf("ERROR Rescan_OV#7 @line=%d axref len_xrefdata=%d!=2", lines, len_xrefdata)
							return false, 0
						}

						xrefgroup := xrefdata[0]
						if x == 1 && xrefgroup != group {
							log.Printf("ERROR Rescan_OV#7a @line=%d xrefgroup != group", lines)
							return false, 0
						}
						if !IsValidGroupName(xrefgroup) {
							log.Printf("ERROR Rescan_OV#7b @line=%d axref !IsValidGroupName", lines)
							return false, 0
						}
						xrefmsgnum := utils.Str2uint64(xrefdata[1])
						if xrefmsgnum == 0 || xrefmsgnum != msgnum {
							log.Printf("ERROR Rescan_OV#7c @line=%d axref xrefmsgnum=%d != msgnum=%d", lines, xrefmsgnum, msgnum)
							return false, 0
						}

					} // end for xrefs
				} // end check xrefs
			} // end if tabs == overview_tabs

			last_line, last_newlines, last_tabs, last_beg = line, newlines, tabs, position-len(line) // capture
			line, newlines, tabs = "", 0, 0                                                          // reset looped values and try to find next line

		default:
			if frees == 0 {
				line = line + string(c)
			}
		} // end switch char of ovfh.Mmap_handle
		position++

	} // end for rescan_OV

	if db != nil {
		defer db.Close()
	}

	if mode == 1000 {
		if hash2sql != nil {
			*hash2sql <- msgidhashmap
		}

		/*
		if db != nil {
			defer db.Close()
			for key, list := range msgidhashmap {
				if retbool, sqlerr := MsgIDhash2mysqlMany(key, list, db, 0); sqlerr != nil || !retbool {

					if driverErr, isErr := sqlerr.(*mysql.MySQLError); isErr {
						switch driverErr.Number {
							case 1062:
								// duplicate entry to mysql
								sql_dup++
								//last_line, last_newlines, last_tabs, last_beg = line, newlines, tabs, position-len(line) // capture
								//line, newlines, tabs = "", 0, 0                                                          // reset looped values and try to find next line
								//position++
								//continue rescan_OV
							default:
								log.Printf("mysql driverErr='%v' num=%d sqlerr='%v'", driverErr, driverErr.Number, sqlerr)
						}
					}
					*
					log.Printf("ERROR rescan_overview group='%s' MsgIDhash2mysqlMany uncatched return", group)
					os.Exit(1)
					return false, last_msgnum
				}
				// inserted to mysql, including ignored duplicate errors
				sql_ins += len(list)
			}
		}
		log.Printf("Rescan_OV mode=1000 MsgIDhash2mysqlMany returned group='%s' sql_ins=%d", group, sql_ins)
		*/
		return true, last_msgnum
	}

	var gibb int
	toend := len_mmap - position
	last_end := last_beg + len(last_line)

	if !badfooter && toend != OV_RESERVE_END {
		badfooter = true
	}

	str_last := fmt.Sprintf("last[ msgnum=%d newlines=%d tabs=%d beg=%d len=%d end=%d ]",
		last_msgnum, last_newlines, last_tabs, last_beg, len(last_line), last_end)
	log.Printf(" ---> result OV lines=%d %s", lines, str_last)

	if badfooter {

		log.Printf(" ---> good last_line='%s'", last_line)
		log.Printf(" ---> prev [newlines=%d, tabs=%d]", newlines, tabs)
		if toend != OV_RESERVE_END {
			fix_flag = "fix-footer"
			log.Printf(" ---> position=%d/%d toend=%d != OV_RESERVE_END=%d", position, len_mmap, toend, OV_RESERVE_END)
		}
		log.Printf(" ---> badfooter=%t", badfooter)
		log.Printf(" ---> end_body=%d", last_newline_pos-frees)
		log.Printf(" ---> last_newline_pos=%d toend=%d", last_newline_pos, len_mmap-last_newline_pos)

		if newlines == 0 || tabs != OVERVIEW_TABS {
			gibb = len_mmap - 1 - last_end
			log.Printf(" ---> ERROR: more data after last_line")
			log.Printf(" ---> gibberish=%d", gibb)
			log.Printf(" ---> bad line='%s'", line)
			log.Printf(" ---> EoL bad len_line=%d", len(line))
			if gibb == len(line) && fix_flag == "" {
				fix_flag = "fix-footer"
			}
			if mode != 999 {
				log.Printf(" ----> CONSIDER: delete the broken line and run fix-footer 'mode=999'")
				return false, 0
			} else {
				log.Printf(" WARN 'mode=999' will try fix-footer fp='%s'", filepath.Base(file_path))
				//time.Sleep(5 * time.Second)
			}
		}
	} // end if badfooter

	// checks footer at the end only mode 0, 4 or 999
	if !badfooter && (mode == 0 || mode == 4 || mode == 999) {
		ov_footer_line := string(ovfh.Mmap_handle[:len_mmap-OV_RESERVE_END])
		if retbool := check_ovfh_footer(who, ov_footer_line); retbool == false {
			if mode != 999 {
				log.Printf("%s ERROR Rescan_OV check_ovfh_footer retbool=%t", who, retbool)
				return false, 0
			} else {
				badfooter = true
			}
		}
	}
	if mode < 999 {
		return true, last_msgnum
	}

	// fix-footer
	if mode == 999 && badfooter && fix_flag == "fix-footer" {

		new_ovfh := &OVFH{}
		//ovfh.File_path = file_path
		//ovfh.File_handle = file_handle
		//ovfh.Mmap_handle = ovfh.Mmap_handle
		ovfh.Mmap_size = len_mmap
		ovfh.Mmap_range = ovfh.Mmap_size - 1
		//ovfh.Mmap_len = len_mmap-1
		ovfh.Last = last_msgnum + 1
		ovfh.Findex = last_newline_pos + 1 - frees
		who := "rescan"
		delete := true

		log.Printf("Rescan_OV badfooter -> fix-footer: last_msgnum=%d ovfh.Findex=%d", last_msgnum, ovfh.Findex)
		//time.Sleep(5*time.Second)
		preload_zero("PRELOAD_ZERO_1K")
		//preload_zero("PRELOAD_ZERO_4K")
		new_ovfh, err = Grow_ov(who, ovfh, 1, "1K", mode, delete)
		if err != nil || new_ovfh == nil {
			log.Printf("ERROR Rescan_OV -> fix-footer -> Grow_ov err='%v'", err)
			return false, 0
		}
		cancelchan <- struct{}{}
		/*
			if new_ovfh != nil {
				ovfh = new_ovfh
			}
		*/
		if new_ovfh.Time_open > 0 {
			log.Printf("Rescan OV fix-footer OK, closing")
			//time.Sleep(5*time.Second)

			//if err = Close_ov(who, ovfh, false, true); err != nil {
			if err = handle_close_ov(who, new_ovfh, false, true, false); err != nil {
				log.Printf("ERROR Rescan OV fix-footer Close_ov err='%v' fp='%s'", err, filepath.Base(file_path))
				return false, 0
			}
			log.Printf("Rescan OV fix-footer mmap closed OK fp='%s'", filepath.Base(file_path))
			return true, last_msgnum
		}
		log.Printf("Error Final Rescan OV -> Grow_ov returned new_ovfh='%v'", new_ovfh)
	} // end if mode == 999

	log.Printf("ERROR Rescan_OV returned with false badfooter=%t fix_flag='%s'", badfooter, fix_flag)
	return false, 0
} // end func Rescan_Overview

func FilterMessageID(messageid string) bool {
	lmsgid := strings.ToLower(messageid)
	if strings.HasPrefix(lmsgid, "<part") ||
		//strings.HasSuffix(lmsgid, "googlegroups.com>") ||
		strings.HasSuffix(lmsgid, "@nyuu>") ||
		strings.HasSuffix(lmsgid, "@ngpost>") ||
		strings.HasSuffix(lmsgid, "@jbinup.local>") ||
		strings.HasSuffix(lmsgid, "@jbindown.local>") ||
		strings.HasSuffix(lmsgid, "@powerpost2000aa.local>") ||
		strings.Contains(lmsgid, "@nyuu") ||
		strings.Contains(lmsgid, "@powerpost") ||
		strings.Contains(lmsgid, "@jbinup") ||
		strings.Contains(lmsgid, "@jbindown") ||
		strings.Contains(lmsgid, "@ngpost") {
		return true
	}
	return false
} // end func FilterMessageID
