package kafka_test

import (
	"testing"

	"github.com/andrewwormald/workflow/connectors/eventstreaming/kafka"
	connector "github.com/andrewwormald/workflow/connectors/testing"
)

func TestStreamer(t *testing.T) {
	constructor := kafka.New([]string{"localhost:9092"})
	connector.TestStreamer(t, constructor)
}
