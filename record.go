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
		WorkflowName: r.WorkflowName,
		ForeignId:    r.ForeignID,
		RunId:        r.RunID,
		Status:       r.Status,
		IsStart:      r.IsStart,
		IsEnd:        r.IsEnd,
		Object:       r.Object,
		CreatedAt:    timestamppb.New(r.CreatedAt),
	}
}

func UnmarshalRecord(b []byte) (*WireRecord, error) {
	var wpb workflowpb.Record
	err := proto.Unmarshal(b, &wpb)
	if err != nil {
		return nil, errors.Wrap(err, "failed to proto marshal entry")
	}

	return &WireRecord{
		WorkflowName: wpb.WorkflowName,
		ForeignID:    wpb.ForeignId,
		RunID:        wpb.RunId,
		Status:       wpb.Status,
		IsStart:      wpb.IsStart,
		IsEnd:        wpb.IsEnd,
		Object:       wpb.Object,
		CreatedAt:    wpb.CreatedAt.AsTime(),
	}, nil
}
