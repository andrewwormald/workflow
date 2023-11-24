package reflexstreamer

import (
	"hash/fnv"

	"github.com/luno/reflex"
)

type EventType int

func (ev EventType) ReflexType() int {
	return int(ev)
}

func TranslateToEventType(parts ...string) (reflex.EventType, error) {
	var b []byte
	for _, s := range parts {
		b = append(b, []byte(s)...)
	}
	h := fnv.New32a()
	_, err := h.Write(b)
	if err != nil {
		return nil, err
	}
	return EventType(h.Sum32()), nil
}
