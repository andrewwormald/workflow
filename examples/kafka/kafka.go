package kafka

import (
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/adapters/kafkastreamer"
	"github.com/andrewwormald/workflow/adapters/memrecordstore"
	"github.com/andrewwormald/workflow/adapters/memrolescheduler"
	"github.com/andrewwormald/workflow/adapters/memtimeoutstore"
	"github.com/andrewwormald/workflow/examples/gettingstarted"
)

func KafkaExampleWorkflow() *workflow.Workflow[gettingstarted.GettingStarted, gettingstarted.Status] {
	return gettingstarted.GettingStartedWithEnumWorkflow(gettingstarted.Deps{
		EventStreamer: kafkastreamer.New([]string{"localhost:9092"}),
		RecordStore:   memrecordstore.New(),
		TimeoutStore:  memtimeoutstore.New(),
		RoleScheduler: memrolescheduler.New(),
	})
}
