# nntp-overview

nntp-overview writes and reads the overview (XOVER) data of a usenet server: one file per newsgroup, one line per article.

- **Numbering:** it is the central place that assigns article numbers per group. The numbers stay consecutive, so a group's article number is its line number in the file.
- **Concurrency:** incoming articles are processed by concurrent workers. Each group file is memory-mapped while it is open, and only one worker writes to a group at a time.
- **Crash recovery:** files are built to survive a crash of the process, and broken files can be checked and repaired (see [Crash safety and repair](#crash-safety-and-repair)).

It is used by [nntp-server](https://github.com/go-while/nntp-server) to import old usenet archives (archive.org mbox files, CD-ROMs from the 90s).

## File format

Each group has a file `<sha256(group)>.overview`:

```
[0:128)            header  "#ov_init=<unixtime>,group=<sha256>,zeropad=<NULs>,EOH\n"
[128:Findex)       lines   "<num>\t<Subject>\t<From>\t<Date>\t<Message-ID>\t<References>\t<Bytes>\t<Lines>\tnntp\n"
[Findex:size-128)  free    NUL bytes
[size-128:size)    footer  "\nEOV\ntime=<unixnano>,last=<next>,Findex=<offset>,bodyend=..,fend=..,zeropad=<NULs>,\nEOF\n"
```

- **Lines:** each line has 9 fields separated by 8 tabs, following [overview.fmt](https://datatracker.ietf.org/doc/html/rfc2980#section-2.1.7). The first field is the article number; the Xref field is always `nntp`.
- **Field cleanup:** tab, CR, LF and NUL inside a field are replaced with a space before the line is written. All other bytes, including non-UTF-8, are kept.
- **Footer values:**
  - `last` is the **next** article number: the number of the last line plus 1.
  - `Findex` is the offset right after the last line.
  - `bodyend`/`fend` are not trusted when a file is opened: files grown by older versions have wrong values there.
- **Growing:** when the free space runs out, the file grows, usually by 128 KB.

## Usage

```go
import (
	"github.com/go-while/go-utils"
	overview "github.com/go-while/nntp-overview"
)

stop := make(chan bool, 1) // close it on shutdown
ok := overview.Overview.Load_Overview(
	128,   // maxworkers: workers processing incoming headers
	128,   // max_queue_size: size of the input channel Overview.OVIC
	128,   // max_open_mmaps: limit of open overview files
	1000,  // known_messageids: size hint of the known message-id cache
	128,   // ov_opener: handlers for open requests (= maxworkers)
	129,   // ov_closer: handlers for close requests (= maxworkers+1)
	false, // close_always: close the file after every line
	false, // more_parallel: write to the groups of an article in parallel
	stop,
	false, // debug
)
overview.Known_msgids.ExpireThread() // forget known message-ids after they expired

// one article: extract the overview values from its header lines
ovl := overview.Extract_overview(messageid, headlines)
if ovl.Checksum != overview.OVL_CHECKSUM {
	// Subject, From, Date, Message-ID or Newsgroups missing
}
ovl.ReaderCachedir = "/news/ov" // directory of the .overview files
ovl.Grouphashs = make(map[string]string)
for _, group := range ovl.Newsgroups {
	ovl.Grouphashs[group] = utils.Hash256(group)
}
ovl.Retchan = make(chan []*overview.ReturnChannelData, 1)
overview.Overview.OVIC <- ovl
for _, res := range <-ovl.Retchan {
	// res.Newsgroup got article number res.Msgnum
}

// reading: XOVER lines of a group, articles a to b (b == 0: to the end)
lines, err := overview.Scan_Overview(file, group, a, b, "all", nil, "", nil)

// shutdown: after the frontend stopped sending to OVIC
close(stop)
overview.Watch_overview_Workers(128) // waits until all queues are empty and the workers are done
```

`Extract_overview` accepts header lines written as `Key: value`, `Key:value` or `Key : value`, and it ignores the case of the header names.

## Crash safety and repair

What happens to an overview file when the process stops at any point:

- **Kill or crash:** if the process is killed or crashes at any point (also while a file grows), the file can still be opened.
- **Uncommitted line:** a line written but not yet committed to the footer is dropped when the file is opened again. The article was not acknowledged, so the sender delivers it again.
- **Older files:** files written by versions before Oct 2026 can have `last` equal to the last line number instead of last + 1. They open normally and the value is corrected.
- **Auto-repair:** a file that still can not be opened is repaired automatically with `RepairOverview`, and the original is kept as `<file>.bak.<unixtime>`. Set `overview.AutoRepair = false` to turn this off.

### ov-repair

`cmd/ov-repair` checks and repairs files offline. **Stop the server first.**

```
go build -o ov-repair ./cmd/ov-repair

ov-repair /news/ov                # dry run: quick check of header, footer and last line
ov-repair -full /news/ov          # dry run: check every line
ov-repair -fix /news/ov           # repair, keep the original as <file>.bak.<unixtime>
ov-repair -fix -q /news/ov        # only print files that are not OK
```

A repair rebuilds the file from its header and all valid lines:
- torn lines and leftovers of old footers are dropped;
- lines that reuse an article number are moved to the end and get new numbers;
- a fresh free area and footer are written.

Each file gets one of these statuses:

| Status | Meaning |
|---|---|
| `OK` | nothing to do |
| `FIXABLE` | `last` off by one or stale bytes after `Findex`: fixed automatically when the file is opened |
| `BROKEN` | needs `-fix` |
| `REPAIRED` | rewritten by `-fix` |
| `BROKEN_HEADER` | invalid header: not repaired, check it manually |
| `ERROR` | i/o error |

The exit code is 0 when every file is OK or was repaired, 1 when files still need a repair, and 2 for a usage error.

A repair renames the group's `.Index` file to `.Index.bak.<unixtime>`, because its offsets no longer match.

## Other parts

- **Known message-ids** (`Known_msgids`): a short-lived cache of message-id hashes that are being received, so two connections can not deliver the same article at the same time.
- **Hash database** (`mySQL.go`, `msgidhash.sql`): optional MySQL table of message-id hashes, used for duplicate detection and reject markers.
- **Overview index** (`Overview_Index.go`): `<file>.Index` with offsets for every 100 articles, so XOVER on large groups does not read from the start.
- **ReOrderOverview**: writes `<file>.new` with the lines sorted by Date and renumbered. Lines with an unparseable Date keep the position of the line before them. The new file is not renamed over the original.
- **Rescan_Overview**: older check and rebuild modes (`Rescan_help()` lists them).
- **Spamfilter**: simple subject/from filter used by ReOrderOverview.

## Tests

```
go test ./...
```

The tests build broken files by hand (footer off by one, no footer, torn lines, reused numbers, stale bytes) and check that the files are repaired correctly. They also simulate a crash after every write through several grows, by copying the live file and opening the copy.

## Contributing

Pull requests are welcome.

For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)

## Author

[go-while](https://github.com/go-while)
