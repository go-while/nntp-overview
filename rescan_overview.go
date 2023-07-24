package overview

import (
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/go-while/go-utils"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	os.Exit(0)
}

// Rescan_Overview returns: true|false, last_msgnum
func Rescan_Overview(file_path string, group string, mode int, DEBUG bool) (bool, uint64) {
	// the steps are:
	// check header
	// check footer ('check footer' runs before 'check lines' or at the end if footer is broken, use mode=4 setting)
	// check lines
	// verify fields
	// check footer

	if DEBUG_OV {
		log.Printf(" -> Start Rescan_Overview: fp='%s' group='%s' mode=%d", file_path, group, mode)
	}

	var err error
	var fix_flag string
	var file_handle *os.File
	var mmap_handle mmap.MMap

	if file_handle, mmap_handle, err = utils.MMAP_FILE(file_path, "ro"); err != nil {
		log.Printf("ERROR Rescan_OV MMAP_FILE err='%v'", err)
		return false, 0
	}
	defer utils.MMAP_CLOSE(file_path, file_handle, mmap_handle, "ro")
	len_mmap := len(mmap_handle)

	if len_mmap < 1024+OV_RESERVE_BEG+OV_RESERVE_END {
		log.Printf("ERROR Rescan_OV len_mmap=%d <= 1024+OV_RESERVE_BEG+OV_RESERVE_END", len_mmap)
		log.Printf("CONSIDER: delete .overview file because is too small or empty?")
		return false, 0
	}

	startindex := OV_RESERVE_BEG
	//endindex := len_mmap - OV_RESERVE_END
	endindex := len_mmap - 1
	if DEBUG_OV {
		log.Printf(" --> rescan_OV mode=%d len=%d startindex=%d", mode, len_mmap, startindex)
	}

	if mode == 0 || mode == 1 || mode == 997 || mode == 998 {
		ov_header_line := string(mmap_handle[:OV_RESERVE_BEG])
		if retbool := check_ovfh_header(ov_header_line); retbool == false {
			log.Printf("ERROR Rescan_OV check_ovfh_header retbool=%t", retbool)
			return false, 0
		}
	}
	if mode == 1 {
		return true, 0
	}

	if mode == 0 || mode == 2 || mode == 997 || mode == 998 {
		ov_footer_line := string(mmap_handle[len_mmap-OV_RESERVE_END:])
		if retbool := check_ovfh_footer(ov_footer_line); retbool == false {
			log.Printf("ERROR Rescan_OV check_ovfh_footer retbool=%t footer='%s'", retbool, string(ov_footer_line))
			return false, 0
		}

	}
	if mode == 2 {
		return true, 0
	}

	line, lines, newlines, tabs, frees, position := "", uint64(0), 0, 0, 0, OV_RESERVE_BEG
	last_line, last_msgnum, last_newlines, last_tabs, last_beg, last_newline_pos := line, lines, newlines, tabs, position, position
	uniq_msgids := make(map[string]uint64)
	var list_msgids []string

rescan_OV:
	for i := startindex; i <= endindex; i++ {
		c := mmap_handle[i] // byte char at index i

		switch c { // check what this byte char is
		case 0:
			frees++

		case '\t':
			tabs++
			line = line + string(c)

		case '\n':
			newlines++
			// found end of line
			if frees > 0 {
				// if we found a nul byte there should only be nuls before
				// check this + next 5 bytes for \nEOV\n (BODY_END)
				// further check FOOTER_BEG and verify content from 'time=....,zeropad=,\nEOF\n'
				to := i + 5
				if to > len_mmap-1 {
					log.Printf("ERROR Rescan_OV#0a frees=%d pos=%d i=%d to=%d len_mmap=%d reached end-of-file ... footer missing?", frees, position, i, to, len_mmap)
					if mode == 999 {
						fix_flag = "fix-footer"
						break rescan_OV
					}
					return false, 0
				}
				check_str := string(mmap_handle[i:to])
				if check_str != BODY_END {
					log.Printf("ERROR Rescan OV#0b check_str='%s'", check_str)
					return false, 0
				}
				end := len_mmap - 1 - 3
				rem := string(mmap_handle[to : end-1]) // to before the 'EOF'
				eof := string(mmap_handle[end-1:])     // should be '\nEOF\n'
				// check backwards for <nul>,
				if rem[len(rem)-2] == 0 && rem[len(rem)-1] == ',' && eof == FOOTER_END {
					// capture footer content
					ov_footer := rem

					// verify footer FIXME TODO: add into a function
					if !strings.HasPrefix(ov_footer, "time=") {
						log.Printf("ERROR Rescan_OV ov_footer='%s' fp='%s'", ov_footer, filepath.Base(file_path))
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
					if DEBUG_OV {
						log.Printf(" --> Rescan_OV footer f_time=%d age=%d fp='%s'", f_time/1e9, last_modified, filepath.Base(file_path))
					}

					if f_last == 0 || f_last-1 != last_msgnum {
						log.Printf("ERROR Rescan_OV footer last=%d != last_msgnum=%d fp='%s'", f_last, last_msgnum, filepath.Base(file_path))
						return false, 0
					}
					if DEBUG_OV {
						log.Printf(" --> Rescan_OV footer last=OK=%d  fp='%s'", f_last, filepath.Base(file_path))
					}

					if f_indx != last_newline_pos+1 {
						diff := f_indx - last_newline_pos
						log.Printf("ERROR Rescan_OV footer Findex=%d != last_newline_pos=%d+1 diff=%d fp='%s'", f_indx, last_newline_pos, diff, filepath.Base(file_path))
						return false, 0
					}
					if DEBUG_OV {
						log.Printf(" --> Rescan_OV footer Findex=OK=%d fp='%s'", f_indx, filepath.Base(file_path))
					}

					if f_fend-f_bodyend != OV_RESERVE_END {
						log.Printf("ERROR Rescan_OV footer f_fend=%d - f_bodyend=%d ==%d != OV_RESERVE_END=%d fp='%s'", f_fend, f_bodyend, f_fend-f_bodyend, OV_RESERVE_END, filepath.Base(file_path))
						return false, 0
					}
					if f_zero == 0 {
						log.Printf("ERROR Rescan_OV footer f_zero=%d fp='%s'", f_zero, filepath.Base(file_path))
						return false, 0
					}
					if DEBUG_OV {
						log.Printf(" --> Rescan_OV footer f_zero=%d fp='%s'", f_zero, filepath.Base(file_path))
					}

					if DEBUG_OV {
						log.Printf(" > OK ov_footer='%s' len=%d fp='%s'", ov_footer, len(ov_footer), filepath.Base(file_path))
						log.Printf(" OK Rescan_OV fp='%s'", filepath.Base(file_path))
					}
					return true, last_msgnum
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
				if len_fields != OVERVIEW_FIELDS {
					log.Printf("ERROR Rescan_OV#3 @line=%d newlines=%d fields=%d!=%d tabs=%d startindex=%d position=%d line='%s'", lines, newlines, len_fields, OVERVIEW_FIELDS, tabs, startindex, position, line)
					return false, 0
				}
				lines++ // raise overview line counter
				msgnum = utils.Str2uint64(fields[0])
				if msgnum != lines {
					log.Printf("ERROR Rescan_OV#4 @line=%d msgnum=%d", lines, msgnum)
					return false, 0
				}
				// be verbose?
				if DEBUG_OV {
					log.Printf(" ----> Rescan: line=%d msgnum=%d tabs=%d newslines=%d len=%d pos=%d last_newline_pos=%d", lines, msgnum, tabs, newlines, len(line), position, last_newline_pos)
				}
				last_msgnum = msgnum
				//last_line = line
			} else {
				log.Printf("WARN Rescan_OV#2 @line=%d found newline but tabs=%d startindex=%d pos=%d frees=%d line='%s'", lines, tabs, startindex, position, frees, line)
				//return false
			}

			if mode == 3 || mode == 997 {
				last_line, last_newlines, last_tabs, last_beg = line, newlines, tabs, position-len(line) // capture
				line, newlines, tabs = "", 0, 0                                                          // reset looped values and try to find next line
				position++
				continue rescan_OV
			}

			if tabs == OVERVIEW_TABS && mode >= 998 {
				// deep verify scan of fields

				/*
					Subject         string      1
					From            string      2
					Date            string      3
					Messageid       string      4
					Bytes           int64       5
					Lines           int         6
					References      []string    7
					Xref            string      8
				*/
				msgid := fields[4]
				bytes := utils.Str2int(fields[5])
				deezlines := utils.Str2int(fields[6])
				full_xref_str := fields[8]

				// start verify fields
				if !isvalidmsgid(msgid, false) {
					log.Printf("ERROR Rescan_OV#5 @line=%d !isvalidmsgid msgnum=%d", lines, msgnum)
					return false, 0
				}
				if uniq_msgids[msgid] > 0 {
					log.Printf("ERROR Rescan_OV#5a @line=%d !uniq_msgid msgnum=%d msgid='%s' firstL=%d", lines, msgnum, msgid, uniq_msgids[msgid])
					return false, 0
				}
				uniq_msgids[msgid] = lines
				list_msgids = append(list_msgids, msgid)

				if bytes <= 0 && deezlines <= 0 {
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
						if xrefmsgnum != msgnum {
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
		} // end switch char of mmap_handle
		position++

	} // end for rescan_OV

	badfooter := false
	var gibb int
	toend := len_mmap - position
	last_end := last_beg + len(last_line)

	if toend != OV_RESERVE_END {
		badfooter = true
	}

	if badfooter {
		str_last := fmt.Sprintf("last[ msgnum=%d newlines=%d tabs=%d beg=%d len=%d end=%d ]",
			last_msgnum, last_newlines, last_tabs, last_beg, len(last_line), last_end)

		log.Printf(" ---> result OV lines=%d %s", lines, str_last)
		log.Printf(" ---> good last_line='%s'", last_line)
		log.Printf(" ---> prev [newlines=%d, tabs=%d]", newlines, tabs)
		log.Printf(" ---> position=%d/%d toend=%d != OV_RESERVE_END=%d", position, len_mmap, toend, OV_RESERVE_END)
		log.Printf(" ---> badfooter=%t", badfooter)
		log.Printf(" ---> last_newline_pos=%d", last_newline_pos)

		if newlines == 0 || tabs != OVERVIEW_TABS {
			gibb = len_mmap - 1 - last_end
			log.Printf(" ---> ERROR: more data after last_line")
			log.Printf(" ---> gibberish=%d", gibb)
			log.Printf(" ---> bad line='%s'", line)
			log.Printf(" ---> EoL bad len_line=%d", len(line))
			if gibb == len(line) {
				fix_flag = "fix-footer"
			}
			if mode != 999 {
				log.Printf(" ----> CONSIDER: delete the broken line and run fix-footer 'mode=999'")
				return false, 0
			} else {
				log.Printf(" WARN 'mode=999' will try fix-footer in 5s fp='%s'", filepath.Base(file_path))
				time.Sleep(5 * time.Second)
			}
		}
	} // end if badfooter

	// checks footer at the end only mode 0, 4 or 999
	if !badfooter && (mode == 0 || mode == 4 || mode == 999) {
		ov_footer_line := string(mmap_handle[:len_mmap-OV_RESERVE_END])
		if retbool := check_ovfh_footer(ov_footer_line); retbool == false {
			if mode != 999 {
				log.Printf("ERROR Rescan_OV check_ovfh_footer retbool=%t", retbool)
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

		log.Printf("Rescan_OV badfooter -> fix-footer")

		var ovfh OVFH
		ovfh.File_path = file_path
		ovfh.File_handle = file_handle
		ovfh.Mmap_handle = mmap_handle
		ovfh.Mmap_size = len_mmap
		ovfh.Mmap_range = ovfh.Mmap_size - 1
		//ovfh.Mmap_len = len_mmap-1
		ovfh.Last = last_msgnum + 1
		ovfh.Findex = last_newline_pos + 1

		if ovfh, err = Grow_ov(ovfh, 1, "1K", mode); err != nil {
			log.Printf("ERROR Rescan_OV -> fix-footer -> Grow_ov err='%v'", err)
			return false, 0
		}

		if ovfh.Time_open > 0 {
			log.Printf("Rescan OV fix-footer OK, closing")
			if err = Close_ov(ovfh, false, true); err != nil {
				log.Printf("ERROR Rescan OV fix-footer Close_ov err='%v' fp='%s'", err, filepath.Base(file_path))
				return false, 0
			}
			log.Printf("Rescan OV fix-footer mmap closed OK fp='%s'", filepath.Base(file_path))
			return true, last_msgnum
		}

	} // end if mode == 999

	log.Printf("ERROR Rescan_OV returned with false badfooter=%t fix_flag='%s'", badfooter, fix_flag)
	return false, 0
} // end func Rescan_Overview
