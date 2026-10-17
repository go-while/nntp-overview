// ov-repair checks and repairs nntp-overview files (<sha256(group)>.overview).
//
// The nntp-server must be stopped while files are repaired.
//
//	ov-repair [-fix] [-full] [-backup=true] [-q] <file.overview|dir>...
//
// Without -fix nothing is written (dry run).
// A directory is scanned for *.overview files (not recursive).
//
// Exit codes: 0 all files OK (or repaired), 1 files need repair or a repair failed, 2 usage error.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	overview "github.com/go-while/nntp-overview"
)

func main() {
	fix := flag.Bool("fix", false, "repair broken files (default: dry run, only report)")
	full := flag.Bool("full", false, "check every line (default: quick check of header, footer and last line)")
	backup := flag.Bool("backup", true, "keep the original file as <file>.bak.<unixts> when repairing")
	quiet := flag.Bool("q", false, "only print files that are not OK")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [-fix] [-full] [-backup=true] [-q] <file.overview|dir>...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  the nntp-server must be stopped while files are repaired!\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	var files []string
	for _, arg := range flag.Args() {
		st, err := os.Stat(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR %v\n", err)
			os.Exit(2)
		}
		if !st.IsDir() {
			files = append(files, arg)
			continue
		}
		matches, err := filepath.Glob(filepath.Join(arg, "*.overview"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR %v\n", err)
			os.Exit(2)
		}
		files = append(files, matches...)
	}
	sort.Strings(files)

	counts := map[string]int{}
	exitcode := 0
	for _, file := range files {
		if !strings.HasSuffix(file, ".overview") {
			fmt.Fprintf(os.Stderr, "SKIP %s: not a .overview file\n", file)
			continue
		}
		rep := overview.CheckOverview(file, *full)
		if *fix && rep.Status != overview.REPAIR_STATUS_OK && rep.Status != overview.REPAIR_STATUS_BROKEN_HEADER {
			var err error
			rep, err = overview.RepairOverview(file, overview.RepairOpts{Fix: true, Backup: *backup})
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR repair %s: %v\n", file, err)
			}
		}
		counts[rep.Status]++
		switch rep.Status {
		case overview.REPAIR_STATUS_OK, overview.REPAIR_STATUS_REPAIRED:
		default:
			exitcode = 1
		}
		if !*quiet || rep.Status != overview.REPAIR_STATUS_OK {
			fmt.Println(rep.String())
		}
	}

	var summary []string
	for status, n := range counts {
		summary = append(summary, fmt.Sprintf("%s=%d", status, n))
	}
	sort.Strings(summary)
	mode := "dry-run"
	if *fix {
		mode = "fix"
	}
	fmt.Printf("# files=%d mode=%s full=%t %s\n", len(files), mode, *full, strings.Join(summary, " "))
	os.Exit(exitcode)
}
