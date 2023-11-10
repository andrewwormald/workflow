package workflow

import (
	"time"

	"github.com/luno/jettison/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/andrewwormald/workflow/workflowpb"
)

type Record[T any, Status ~string] struct {
	WireRecord
	Status Status
	Object *T
}

type WireRecord struct {
	WorkflowName string
	ForeignID    string
	RunID        string
	Status       string
	IsStart      bool
	IsEnd        bool
	Object       []byte
	CreatedAt    time.Time
}

func (r *WireRecord) ProtoMarshal() ([]byte, error) {
	pb, err := proto.Marshal(ToProto(r))
	if err != nil {
		return nil, errors.Wrap(err, "failed to proto marshal entry")
	}

	return pb, nil
}

func ToProto(r *WireRecord) *workflowpb.Record {
	return &workflowpb.Record{
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
