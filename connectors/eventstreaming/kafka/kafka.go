package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/andrewwormald/workflow"
	"github.com/google/uuid"
	"github.com/luno/jettison/errors"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func New() *StreamConstructor {
	return &StreamConstructor{
		brokers: []string{"localhost:9092"},
	}
}

var _ workflow.EventStreamerConstructor = (*StreamConstructor)(nil)

type StreamConstructor struct {
	brokers []string
}

func formTopic(workflowName, status string) string {
	workflowName = strings.ReplaceAll(workflowName, " ", "-")
	status = strings.ReplaceAll(status, " ", "-")
	topic := fmt.Sprintf("%v-%v", workflowName, status)
	return topic
}

func (s StreamConstructor) NewProducer(workflowName string, status string) workflow.Producer {
	topic := formTopic(workflowName, status)
	return &Producer{
		Topic: topic,
		Writer: &kafka.Writer{
			Addr:                   kafka.TCP(s.brokers...),
			Topic:                  topic,
			AllowAutoTopicCreation: true,
		},
		WriterTimeout: 0,
	}
}

func (s StreamConstructor) NewConsumer(workflowName string, status string) workflow.Consumer {
	topic := formTopic(workflowName, status)
	return &Consumer{
		Topic: topic,
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     s.brokers,
			GroupID:     uuid.New().String(),
			Topic:       topic,
			StartOffset: kafka.LastOffset,
		}),
	}
}

var _ workflow.EventStreamerConstructor = (*StreamConstructor)(nil)

type Producer struct {
	Topic         string
	Writer        *kafka.Writer
	WriterTimeout time.Duration
}

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

var _ workflow.Producer = (*Producer)(nil)

type Consumer struct {
	Topic  string
	Reader *kafka.Reader
}

func (c *Consumer) Recv(ctx context.Context) (*workflow.WireRecord, workflow.Ack, error) {
	m, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		return nil, nil, err
	}

	fmt.Println(m.Offset)

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
