package bridge

import (
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// chunkReader yields at most n bytes per Read, to exercise parsing
// across arbitrary chunk boundaries (including mid-line and mid-event).
type chunkReader struct {
	r io.Reader
	n int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(p) > c.n {
		p = p[:c.n]
	}
	return c.r.Read(p)
}

func collect(t *testing.T, r io.Reader) []Event {
	t.Helper()
	var got []Event
	if err := readEvents(r, func(ev Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatalf("readEvents: %v", err)
	}
	return got
}

func TestReadEvents(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []Event
	}{
		{
			name: "single event",
			in:   "data: {\"a\":1}\n\n",
			want: []Event{{Data: []byte(`{"a":1}`)}},
		},
		{
			name: "multiple events with fields and comment",
			in:   "event: message\nid: 1\ndata: one\n\ndata: two\n\n: comment line\ndata: three\n\n",
			want: []Event{
				{Name: "message", ID: "1", Data: []byte("one")},
				{Data: []byte("two")},
				{Data: []byte("three")},
			},
		},
		{
			name: "multi-line data joined with newline",
			in:   "data: line1\ndata: line2\n\n",
			want: []Event{{Data: []byte("line1\nline2")}},
		},
		{
			name: "CRLF terminators",
			in:   "data: x\r\n\r\n",
			want: []Event{{Data: []byte("x")}},
		},
		{
			// Lenient handling of streams that end without a final
			// dispatch line.
			name: "no trailing blank line",
			in:   "data: tail",
			want: []Event{{Data: []byte("tail")}},
		},
		{
			name: "empty stream",
			in:   "",
			want: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := collect(t, strings.NewReader(c.in))
			if diff := cmp.Diff(c.want, got); diff != "" {
				t.Errorf("events mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReadEventsChunkBoundaries(t *testing.T) {
	in := "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1}\n\ndata: second\n\n"
	want := []Event{
		{Name: "message", Data: []byte(`{"jsonrpc":"2.0","id":1}`)},
		{Data: []byte("second")},
	}
	for _, n := range []int{1, 2, 3, 7} {
		got := collect(t, &chunkReader{r: strings.NewReader(in), n: n})
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("chunk size %d: events mismatch (-want +got):\n%s", n, diff)
		}
	}
}
