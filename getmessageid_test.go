package overview

import "testing"

func TestGetMessageID(t *testing.T) {
	cases := []struct {
		in       string
		lax      string
		laxOK    bool
		strict   string
		strictOK bool
	}{
		{"", "", false, "", false},
		{"<a@b>", "<a@b>", true, "<a@b>", true},
		{" <a@b> ", "<a@b>", true, "<a@b>", true},
		{"<msgid@x>", "<msgid@x>", true, "<msgid@x>", true},
		{"<ab>", "<ab>", true, "", false},       // no "@"
		{"<a b@c>", "<a b@c>", true, "", false}, // a space
		{"<a@b>#1/1", "<a@b>", true, "", false}, // lax strips the suffix
		{"<a@b> (x)", "<a@b>", true, "", false}, // lax drops the comment
		{"<a@b>?(x)", "<a@b>", true, "", false},
		{"<a@b>(UMass-2)", "<a@b>", true, "", false},
		{"<a@b>-(UMass-2)", "<a@b>", true, "", false},
		{"a@b", "", false, "", false},
		{"<a@b", "", false, "", false},
	}
	for _, c := range cases {
		lax, laxErr := GetMessageID(c.in, true)
		strict, strictErr := GetMessageID(c.in, false)
		if lax != c.lax || (laxErr == nil) != c.laxOK {
			t.Errorf("GetMessageID(%q, lax) = %q, %v; want %q, ok=%t", c.in, lax, laxErr, c.lax, c.laxOK)
		}
		if strict != c.strict || (strictErr == nil) != c.strictOK {
			t.Errorf("GetMessageID(%q, strict) = %q, %v; want %q, ok=%t", c.in, strict, strictErr, c.strict, c.strictOK)
		}
	}
}
