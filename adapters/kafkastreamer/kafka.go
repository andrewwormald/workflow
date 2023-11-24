package kafkastreamer

import (
	"context"
	"fmt"
	"github.com/andrewwormald/workflow"
	"github.com/luno/jettison/errors"
	"time"

	"github.com/segmentio/kafka-go"
)

func New(brokers []string) *StreamConstructor {
	return &StreamConstructor{
		brokers: brokers,
	}
}

var _ workflow.EventStreamer = (*StreamConstructor)(nil)

type StreamConstructor struct {
	brokers []string
}

func (s StreamConstructor) NewProducer(topic string) workflow.Producer {
	return &Producer{
		Topic: topic,
		Writer: &kafka.Writer{
			Addr:                   kafka.TCP(s.brokers...),
			Topic:                  topic,
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafka.RequireOne,
		},
		WriterTimeout: time.Second * 10,
	}
}

type Producer struct {
	Topic         string
	Writer        *kafka.Writer
	WriterTimeout time.Duration
}

var _ workflow.Producer = (*Producer)(nil)

func (p *Producer) Send(ctx context.Context, e *workflow.Event) error {
	for ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(ctx, p.WriterTimeout)
		defer cancel()

		msgData, err := e.ProtoMarshal()
		if err != nil {
			return err
		}

		var headers []kafka.Header
		for key, value := range e.Headers {
			headers = append(headers, kafka.Header{
				Key:   key,
				Value: []byte(value),
			})
		}

		key := fmt.Sprintf("%v", e.ForeignID)
		msg := kafka.Message{
			Key:     []byte(key),
			Value:   msgData,
			Headers: headers,
		}

		err = p.Writer.WriteMessages(ctx, msg)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		} else if err != nil {
			return err
		}

		break
	}

	return ctx.Err()
}

func (p *Producer) Close() error {
	return p.Writer.Close()
}

func (s StreamConstructor) NewConsumer(topic string, name string, opts ...workflow.ConsumerOption) workflow.Consumer {
	var copts workflow.ConsumerOptions
	for _, opt := range opts {
		opt(&copts)
	}

	startOffset := kafka.FirstOffset

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        s.brokers,
		GroupID:        name,
		Topic:          topic,
		ReadBackoffMin: copts.PollFrequency,
		ReadBackoffMax: copts.PollFrequency,
		StartOffset:    startOffset,
		QueueCapacity:  1000,
		MinBytes:       10,  // 10B
		MaxBytes:       1e9, // 9MB
		MaxWait:        time.Second,
	})

	return &Consumer{
		topic:   topic,
		name:    name,
		reader:  kafkaReader,
		options: copts,
	}
}

type Consumer struct {
	topic   string
	name    string
	reader  *kafka.Reader
	options workflow.ConsumerOptions
}

func (c *Consumer) Recv(ctx context.Context) (*workflow.Event, workflow.Ack, error) {
	var commit []kafka.Message
	for ctx.Err() == nil {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return nil, nil, err
		}

		// Append the message to the commit slice to ensure we send all messages that have been processed
		commit = append(commit, m)

		e, err := workflow.UnmarshalEvent(m.Value)
		if err != nil {
			return nil, nil, err
		}

		e.ID = m.Offset
		e.CreatedAt = m.Time
		for _, header := range m.Headers {
			e.Headers[header.Key] = string(header.Value)
		}

		if skip := c.options.EventFilter(e); skip {
			continue
		}

		return e, func() error {
				return c.reader.CommitMessages(ctx, commit...)
			},
			nil
	}

	return nil, nil, ctx.Err()
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

var _ workflow.Consumer = (*Consumer)(nil)
