package workflow

import (
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/andrewwormald/workflow/workflowpb"
)

type Record struct {
	ID           int64
	RunID        string
	WorkflowName string
	ForeignID    string
	Status       string
	IsStart      bool
	IsEnd        bool
	Object       []byte
	CreatedAt    time.Time
}

func (r *Record) ProtoMarshal() ([]byte, error) {
	pb, err := proto.Marshal(ToProto(r))
	if err != nil {
		return nil, errors.Wrap(err, "failed to proto marshal entry")
	}

	return pb, nil
}

type TypedRecord[T any] struct {
	ID           int64
	RunID        string
	WorkflowName string
	ForeignID    string
	Status       string
	IsStart      bool
	IsEnd        bool
	Object       T
	CreatedAt    time.Time
}

func Translate[T any](e *reflex.Event) (*TypedRecord[T], error) {
	var r workflowpb.Record
	err := proto.Unmarshal(e.MetaData, &r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to proto unmarshal record")
	}

	var t T
	err = Unmarshal(r.Object, &t)
	if err != nil {
		return nil, err
	}

	return &TypedRecord[T]{
		ID:           r.Id,
		RunID:        r.RunId,
		WorkflowName: r.WorkflowName,
		ForeignID:    r.ForeignId,
		Status:       r.Status,
		IsStart:      r.IsStart,
		IsEnd:        r.IsEnd,
		Object:       t,
		CreatedAt:    r.CreatedAt.AsTime(),
	}, nil
}

func ToProto(r *Record) *workflowpb.Record {
	return &workflowpb.Record{
		Id:           r.ID,
		RunId:        r.RunID,
		WorkflowName: r.WorkflowName,
		ForeignId:    r.ForeignID,
		Status:       r.Status,
		IsStart:      r.IsStart,
		IsEnd:        r.IsEnd,
		Object:       r.Object,
		CreatedAt:    timestamppb.New(r.CreatedAt),
	}
}
