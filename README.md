# Workflow

Workflow is a Golang workflow framework that encompasses n main features:

## Features
- Built in state machine allowing for durable changes with idempotency 
- Built in support for timeout operations (e.g. account cool down periods etc)
- Built in support for callbacks (e.g. Call an async endpoint and trigger the callback from a webhook handler)
- Natively chain workflows together
- Super Duper testable

## Example / Demo 
Here is a fun and simple example of a workflow where we emulate sending a trading report that requires approval to be sent. The demo uses a in-mem implementation of the Store interface but can be easily swapped out for a custom implementation. A mySQL implementation is supported in the `./sqlstore` directory

- You can find the example here: `./example`

##### If you pull the repo, you can run this locally without any setup:
```bash
go run ./example
```
## Step by Step example:
### Step 1: Taking a simple object 
```go
type YinYang struct {
	Yin  bool
	Yang bool
}
```

### Step 2: Building a workflow
```go
b := workflow.NewBuilder[YinYang]("example")
b.AddStep("Start", func(ctx context.Context, key workflow.Key, yy *YinYang) (bool, error) {
    yy.Yin = true
    return true, nil
}, "Middle")

b.AddCallback("Middle", func(ctx context.Context, key workflow.Key, yy *YinYang, r io.Reader) (bool, error) {
    yy.Yang = true
    return true, nil
}, "End")

wf := b.Build(
	store,
	cursor,
	scheduler,
)
```

### Step 3: Call Run to launch the consumers in the background
```go
wf.Run(ctx)
```

### Step 4: Triggering the workflow
```go
foreignID := "andrew@workflow.com"
runID, err := wf.Trigger(ctx, foreignID, "Start")
if err != nil {
    panic(err)
}
```
#### Example of providing a non-zero value starting point of the primary type (YinYang):
```go 
startingValue := &YinYang{
    Yin: true,
}

runID, err := wf.Trigger(ctx, foreignID, "Start", workflow.WithInitialValue(startingValue))
if err != nil {
    panic(err)
}
```
### OR use the cron styled schedule trigger
##### See the standard cron spec below that can be used for scheduling triggers
```go
foreignID := "andrew@workflow.com"
err := wf.ScheduleTrigger(ctx, foreignID, "Start", "@monthly")
if err != nil {
    panic(err)
}
```

| Entry |  Description   |   Equivalent to   |
| :---:   |:---:|:----:|
| @yearly (or @annually) | Run once a year at midnight of 1 January | 0 0 1 1 *  |
| @monthly | Run once a month at midnight of the first day of the month | 0 0 1 * *  |
| @weekly | Run once a week at midnight on Sunday morning | 0 0 * * 0  |
| @daily (or @midnight) | Run once a day at midnight | 0 0 * * *  |
| @hourly | Run once an hour at the beginning of the hour | 0 * * * *  |


### Step 5 A: If you wish for a async await pattern after calling Trigger
```go
yinYang, err := wf.Await(ctx, foreignID, "End")
if err != nil {
    panic(err)
}
```

### Callbacks: Interacting through callbacks are easy peasy
If you were to switch out an automated step for a callback, where the workflow pauses until we get the explicit interaction, then this is what it would look like:

##### Configuring the callback
```go
b.AddCallback("Middle", func(ctx context.Context, key workflow.Key, t *YinYang, r io.Reader) (bool, error) {
    b, err := io.ReadAll(r)
    if err != nil {
        return false, err
    }
	
    var e External
    err = json.Unmarshal(b, &e)
    if err != nil {
        return false, err
    }

    t.Yang = e.Thing == "Some"
    return true, nil
}, "End")
```
##### Calling the callback
```go
type External struct {
    Thing string
}

external := External{
    Thing: "Something",
}

b, err := json.Marshal(external)
if err != nil {
    panic(err)
}

reader := bytes.NewReader(b)

err = wf.Callback(ctx, foreignID, "Middle", reader)
if err != nil {
    panic(err)
}
```

### Timeouts: Scheduling for a future date?
If you were to switch out an automated step for a timeout, a scheduled process, then this is what it would look like:
##### Configuring the timeout
```go
b.AddTimeout(
    "Middle",
    func(ctx context.Context, key workflow.Key, now time.Time) (bool, time.Time, error) {
        
    },
    func(ctx context.Context, key Key, t *string, now time.Time) (bool, error) {
        return true, nil
    },
    "End",
)

b.AddTimeout(
    "Middle",
    workflow.TimeTimerFunc(time.Hour),
    func(ctx context.Context, key Key, t *string, now time.Time) (bool, error) {
        return true, nil
    },
    "End",
)

b.AddTimeout(
    "Middle",
    workflow.DurationTimerFunc(time.Hour),
    func(ctx context.Context, key Key, t *string, now time.Time) (bool, error) {
        return true, nil
    },
    "End",
)
```

## Testing

One core focus of `workflow` is to be easily tested.

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
