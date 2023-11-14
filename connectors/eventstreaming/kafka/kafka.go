package kafka

import (
	"context"
	"encoding/json"
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

var _ workflow.EventStreamerConstructor = (*StreamConstructor)(nil)

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

func (p *Producer) Send(ctx context.Context, r *workflow.WireRecord) error {
	for {
		ctx, cancel := context.WithTimeout(ctx, p.WriterTimeout)
		defer cancel()

		msgData, err := json.Marshal(r)
		if err != nil {
			return err
		}

		key := fmt.Sprintf("%v-%v-%v", r.WorkflowName, r.ForeignID, r.RunID)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: msgData,
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

	return nil
}

func (p *Producer) Close() error {
	return p.Writer.Close()
}

func (s StreamConstructor) NewConsumer(topic string, name string) workflow.Consumer {
	return &Consumer{
		topic: topic,
		name:  name,
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        s.brokers,
			GroupID:        name,
			Topic:          topic,
			ReadBackoffMax: time.Millisecond * 500,
		}),
	}
}

type Consumer struct {
	topic  string
	name   string
	Reader *kafka.Reader
}

func (c *Consumer) Recv(ctx context.Context) (*workflow.WireRecord, workflow.Ack, error) {
	m, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		return nil, nil, err
	}

	var r workflow.WireRecord
	err = json.Unmarshal(m.Value, &r)
	if err != nil {
		return nil, nil, err
	}

	return &r,
		func() error {
			// Leave the committing of the message up to the workflow framework.
			return nil
		},
		nil
}

func (c *Consumer) Close() error {
	return c.Reader.Close()
}

var _ workflow.Consumer = (*Consumer)(nil)
