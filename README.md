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

## Authors

- [@andrewwormald](https://github.com/andrewwormald)
