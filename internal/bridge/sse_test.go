package bridge

import (
	"io"
	"strings"
	"testing"
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

func TestReadEventsSingle(t *testing.T) {
	got := collect(t, strings.NewReader("data: {\"a\":1}\n\n"))
	if len(got) != 1 || string(got[0].Data) != `{"a":1}` {
		t.Fatalf("got %+v", got)
	}
}

func TestReadEventsMultiEvent(t *testing.T) {
	in := "event: message\nid: 1\ndata: one\n\ndata: two\n\n: comment line\ndata: three\n\n"
	got := collect(t, strings.NewReader(in))
	if len(got) != 3 {
		t.Fatalf("want 3 events, got %d: %+v", len(got), got)
	}
	if got[0].Name != "message" || got[0].ID != "1" || string(got[0].Data) != "one" {
		t.Errorf("event 0: %+v", got[0])
	}
	if string(got[1].Data) != "two" || string(got[2].Data) != "three" {
		t.Errorf("events 1/2: %+v %+v", got[1], got[2])
	}
}

func TestReadEventsMultiLineData(t *testing.T) {
	got := collect(t, strings.NewReader("data: line1\ndata: line2\n\n"))
	if len(got) != 1 || string(got[0].Data) != "line1\nline2" {
		t.Fatalf("got %+v", got)
	}
}

func TestReadEventsCRLF(t *testing.T) {
	got := collect(t, strings.NewReader("data: x\r\n\r\n"))
	if len(got) != 1 || string(got[0].Data) != "x" {
		t.Fatalf("got %+v", got)
	}
}

func TestReadEventsChunkBoundaries(t *testing.T) {
	in := "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1}\n\ndata: second\n\n"
	for _, n := range []int{1, 2, 3, 7} {
		got := collect(t, &chunkReader{r: strings.NewReader(in), n: n})
		if len(got) != 2 || string(got[0].Data) != `{"jsonrpc":"2.0","id":1}` || string(got[1].Data) != "second" {
			t.Fatalf("chunk size %d: got %+v", n, got)
		}
	}
}

func TestReadEventsNoTrailingBlankLine(t *testing.T) {
	// Lenient handling of streams that end without a final dispatch line.
	got := collect(t, strings.NewReader("data: tail"))
	if len(got) != 1 || string(got[0].Data) != "tail" {
		t.Fatalf("got %+v", got)
	}
}

func TestReadEventsEmptyStream(t *testing.T) {
	if got := collect(t, strings.NewReader("")); len(got) != 0 {
		t.Fatalf("got %+v", got)
	}
}
