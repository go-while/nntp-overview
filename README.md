# nntp-overview

nntp-overview generates .overview files per group from incoming usenet headers (POST, IHAVE, TAKETHIS).

Generation is done in a concurrent way and files are mmap'ed while open.

Overview file content is human readable based on [RFC overview.FMT](https://datatracker.ietf.org/doc/html/rfc2980#section-2.1.7)
```
    "MSG_NUM:",     // MSG_NUM field is not present in output to overview.FMT command but when requesting XOVER we output it
    "Subject:",
    "From:",
    "Date:",
    "Message-ID:",
    "References:",
    "Bytes:",
    "Lines:",
    "Xref:full",
```

## OV_Handler

OV_Handler processes MMAP open/retrieve/park/close requests and schedules workers for writing overview data.

The system keeps track of last message number per group when adding new overview to group.

When integrated into a usenet server: works as a central message numbering station per group.


## USAGE

```
import (
    "github.com/go-while/go-utils"
    "github.com/go-while/nntp-overview"
)
```

Example integration in repo: [nntp-overview_test](https://github.com/go-while/nntp-overview_test/blob/main/main.go)


## Contributing

Pull requests are welcome.

For major changes, please open an issue first to discuss what you would like to change or fork it and go your way ;)

## License

[MIT](https://choosealicense.com/licenses/mit/)

## Author
[go-while](https://github.com/go-while)
