# Workflow

Workflow is a Golang workflow framework that encompasses these main features:

## Features
- Defining small units of work called "Steps"
- Consumer management and graceful shutdown
- Supports event streaming platforms such as Kafka and Reflex (or you can write your own implementation of the EventStreamer interface!)
- Built in support for timeout operations (e.g. account cool down periods etc).
- Built in support for callbacks (e.g. Call an async endpoint and trigger the callback from a webhook handler).
- Connect two workflows together. Wait for specific events from another workflow and make it part of your workflow!
- Super Duper testable

## Example / Demo

### API
```go
// Trigger will kickstart a workflow for the provided foreignID starting from the provided starting status. There
// is no limitation as to where you start the workflow from. For workflows that have data preceding the initial
// trigger that needs to be used in the workflow, using WithInitialValue will allow you to provide pre-populated
// fields of Type that can be accessed by the consumers.
//
// foreignID should not be random and should be deterministic for the thing that you are running the workflow for.
// This especially helps when connecting other workflows as the foreignID is the only way to connect the streams. The
// same goes for Callback as you will need the foreignID to connect the callback back to the workflow instance that
// was run.
Trigger(ctx context.Context, foreignID string, startingStatus Status, opts ...TriggerOption[Type, Status]) (runID string, err error)

// ScheduleTrigger takes a cron spec and will call Trigger at the specified intervals. The same options are
// available for ScheduleTrigger than they are for Trigger.
ScheduleTrigger(ctx context.Context, foreignID string, startingStatus Status, spec string, opts ...TriggerOption[Type, Status]) error

// Await is a blocking call that returns the typed Record when the workflow of the specified run ID reaches the
// specified status.
Await(ctx context.Context, foreignID, runID string, status Status, opts ...AwaitOption) (*Record[Type, Status], error)

// Callback can be used if Builder.AddCallback has been defined for the provided status. The data in the reader
// will be passed to the CallbackFunc that you specify and so the serialisation and deserialisation is in the
// hands of the user.
Callback(ctx context.Context, foreignID string, status Status, payload io.Reader) error

// Run must be called in order to start up all the background consumers / consumers required to run the workflow. Run
// only needs to be called once. Any subsequent calls to run are safe and are noop.
Run(ctx context.Context)

// Stop tells the workflow to shut down gracefully.
Stop()
```

## Testing

One core focus of `workflow` is to encourage writing tests by making it easy to do so.

The `testing.go` file houses utility functions for testing your workflow. Some other
 useful patterns is to use `k8s.io/utils/clock/testing` testing clock to manipulate
 time and ensure your functions are executing at the exact time and date that they should/

`Require`: Allows for placing assertions or requirements on the state at a specific point in the workflow.
```go
expected := YinYang{
	Yin:   true,
	Yang:  false,
}

wf.Require(t, wf, "Middle", expected)

expected = YinYang{
    Yin:   true,
    Yang:  true,
}

wf.Require(t, wf, "End", expected)
```

`TriggerCallbackOn`: Allows you to easily simulate a callback when the workflow is at a specific
point in its flow.
```go
wf.TriggerCallbackOn(t, wf, fid, "Middle", External{
    Thing: "Something",
})
```

`AwaitTimeoutInsert`: AwaitTimoutInsert helps wait for the timout to be created after which you can use the clock
 to change the time to speed up / skip the timeout process
```go
wf.AwaitTimeoutInsert(t, wf, "Middle")

// Advance time forward by one hour to trigger the timeout
clock.Step(time.Hour)
```

## Authors

- [@andrewwormald](https://github.com/andrewwormald)
