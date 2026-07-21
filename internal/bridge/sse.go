package bridge

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

// Event is a single server-sent event.
type Event struct {
	Name string // "event:" field; empty means the default "message"
	ID   string // "id:" field
	Data []byte // "data:" lines joined with \n
}

// readEvents parses a text/event-stream body per the SSE specification
// (field-per-line, blank-line dispatch, \r\n or \n terminators, ":"
// comment lines) and invokes fn for each event that carries data.
// It returns nil on clean EOF and stops early if fn returns an error.
func readEvents(r io.Reader, fn func(Event) error) error {
	br := bufio.NewReader(r)
	var (
		ev      Event
		data    [][]byte
		hasData bool
	)
	dispatch := func() error {
		if !hasData {
			ev = Event{}
			return nil
		}
		ev.Data = bytes.Join(data, []byte("\n"))
		err := fn(ev)
		ev, data, hasData = Event{}, nil, false
		return err
	}

	for {
		line, err := br.ReadString('\n')
		// Handle a final line without a trailing newline before EOF.
		done := err != nil
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")

		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
		} else if !strings.HasPrefix(line, ":") {
			field, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ")
			switch field {
			case "event":
				ev.Name = value
			case "id":
				ev.ID = value
			case "data":
				data = append(data, []byte(value))
				hasData = true
			}
			// "retry" and unknown fields are ignored.
		}

		if done {
			if err == io.EOF {
				// Per spec an event is only dispatched by a blank line,
				// but be lenient with servers that omit the final one.
				return dispatch()
			}
			return err
		}
	}
}
