package workflowstreamer_test

import (
	"context"
	"github.com/andrewwormald/workflow"
	"github.com/andrewwormald/workflow/memstore"
	"github.com/andrewwormald/workflow/workflowstreamer"
	"github.com/luno/fate"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStreamConversion(t *testing.T) {
	key := workflow.MakeKey("workflow_name", "foreign_id", "run_id")

	var (
		expectedUserID    int64  = 1
		expectedUserName  string = "Andrew Wormald"
		expectedUserEmail string = "andrew@something.com"
	)

	tu := testUser{
		UserID: expectedUserID,
		Email:  expectedUserEmail,
		Name:   expectedUserName,
	}

	b, err := workflow.Marshal(&tu)
	jtest.RequireNil(t, err)

	testCases := []struct {
		name     string
		seed     []func(ctx context.Context, s workflow.Store)
		expected []workflow.Record
	}{
		{
			name: "Started, normal, and completed entry types",
			seed: []func(ctx context.Context, s workflow.Store){
				func(ctx context.Context, s workflow.Store) {
					err := s.Store(ctx, key, "Started", b, true, false)
					jtest.RequireNil(t, err)
				},
				func(ctx context.Context, s workflow.Store) {
					err := s.Store(ctx, key, "Middle", b, false, false)
					jtest.RequireNil(t, err)
				},
				func(ctx context.Context, s workflow.Store) {
					err := s.Store(ctx, key, "Completed", b, false, true)
					jtest.RequireNil(t, err)
				},
			},
			expected: []workflow.Record{
				{
					ID:           1,
					WorkflowName: "workflow_name",
					ForeignID:    "foreign_id",
					RunID:        "run_id",
					Status:       "Started",
					IsStart:      true,
					IsEnd:        false,
					Object:       b,
					CreatedAt:    time.Now(),
				},
				{
					ID:           2,
					WorkflowName: "workflow_name",
					ForeignID:    "foreign_id",
					RunID:        "run_id",
					Status:       "Middle",
					IsStart:      false,
					IsEnd:        false,
					Object:       b,
					CreatedAt:    time.Now(),
				},
				{
					ID:           3,
					WorkflowName: "workflow_name",
					ForeignID:    "foreign_id",
					RunID:        "run_id",
					Status:       "Completed",
					IsStart:      false,
					IsEnd:        true,
					Object:       b,
					CreatedAt:    time.Now(),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := memstore.New()
			ctx := context.Background()
			streamerFunc := workflowstreamer.NewReflexStreamer(store, "workflow_name")

			for _, fn := range tc.seed {
				fn(ctx, store)
			}

			stream, err := streamerFunc(ctx, "")
			jtest.RequireNil(t, err)

			for _, exp := range tc.expected {
				event, err := stream.Recv()
				jtest.RequireNil(t, err)

				require.Equal(t, exp.ID, event.IDInt())
				var expectedType workflowstreamer.RecordType
				switch true {
				case exp.IsStart:
					expectedType = workflowstreamer.RecordTypeStarted
				case exp.IsEnd:
					expectedType = workflowstreamer.RecordTypeCompleted
				default:
					expectedType = workflowstreamer.RecordTypeNormal
				}
				require.Equal(t, expectedType, event.Type)
				require.Equal(t, exp.ForeignID, event.ForeignID)
				require.WithinDuration(t, exp.CreatedAt, event.Timestamp, time.Second)

				entry, err := workflowstreamer.Translate[testUser](event)
				jtest.RequireNil(t, err)

				require.Equal(t, exp.ID, entry.ID)
				require.Equal(t, exp.Status, entry.Status)
				require.Equal(t, exp.WorkflowName, entry.WorkflowName)
				require.Equal(t, exp.ForeignID, entry.ForeignID)
				require.Equal(t, exp.IsStart, entry.IsStart)
				require.Equal(t, exp.IsEnd, entry.IsEnd)
				require.Equal(t, expectedUserID, entry.Object.UserID)
				require.Equal(t, expectedUserName, entry.Object.Name)
				require.Equal(t, expectedUserEmail, entry.Object.Email)
				require.WithinDuration(t, exp.CreatedAt, entry.CreatedAt, time.Second)
			}
		})
	}
}

func TestReflexConsumerWithMemStore(t *testing.T) {
	store := memstore.New()
	testReflexConsumerCompatibility(t, store)
}

func testReflexConsumerCompatibility(t *testing.T, store workflow.Store) {
	ctx := context.Background()

	u := testUser{
		UserID: 9,
	}

	b, err := workflow.Marshal(&u)
	jtest.RequireNil(t, err)

	key := workflow.MakeKey("example", "andrew", "unique")
	err = store.Store(ctx, key, "Started", b, true, false)
	jtest.RequireNil(t, err)

	u = testUser{
		UserID: 9,
		Name:   "andrew",
	}

	b, err = workflow.Marshal(&u)
	jtest.RequireNil(t, err)

	err = store.Store(ctx, key, "Middle", b, false, false)
	jtest.RequireNil(t, err)

	u = testUser{
		UserID: 9,
		Name:   "andrew",
		Email:  "andreww@luno.com",
	}

	b, err = workflow.Marshal(&u)
	jtest.RequireNil(t, err)

	err = store.Store(ctx, key, "Completed", b, false, true)
	jtest.RequireNil(t, err)

	cursor := rpatterns.MemCursorStore()
	var records []workflowstreamer.TypedRecord[testUser]
	run(t, ctx, store, "example", cursor, exampleConsumer(&records))

	require.Equal(t, 3, len(records))
	expected := []testUser{
		{
			UserID: 9,
		},
		{
			UserID: 9,
			Name:   "andrew",
		},
		{
			UserID: 9,
			Name:   "andrew",
			Email:  "andreww@luno.com",
		},
	}

	for i, user := range expected {
		require.Equal(t, user, records[i].Object)
	}
}

func TestStreamFromHeadWithMemStore(t *testing.T) {
	store := memstore.New()
	testStreamFromHead(t, store)
}

func testStreamFromHead(t *testing.T, store workflow.Store) {
	ctx := context.Background()

	u := testUser{
		UserID: 9,
	}

	b, err := workflow.Marshal(&u)
	jtest.RequireNil(t, err)

	key := workflow.MakeKey("example", "andrew", "unique")
	err = store.Store(ctx, key, "Started", b, true, false)
	jtest.RequireNil(t, err)

	u = testUser{
		UserID: 9,
		Name:   "andrew",
	}

	b, err = workflow.Marshal(&u)
	jtest.RequireNil(t, err)

	err = store.Store(ctx, key, "Middle", b, false, false)
	jtest.RequireNil(t, err)

	u = testUser{
		UserID: 9,
		Name:   "andrew",
		Email:  "andreww@luno.com",
	}

	b, err = workflow.Marshal(&u)
	jtest.RequireNil(t, err)

	err = store.Store(ctx, key, "Completed", b, false, true)
	jtest.RequireNil(t, err)

	cursor := rpatterns.MemCursorStore()
	var records []workflowstreamer.TypedRecord[testUser]
	run(t, ctx, store, "example", cursor, exampleConsumer(&records), reflex.WithStreamFromHead())

	require.Equal(t, 1, len(records))

	expected := testUser{
		UserID: 9,
		Name:   "andrew",
		Email:  "andreww@luno.com",
	}
	require.Equal(t, expected, records[0].Object)
}

type testUser struct {
	UserID int64
	Name   string
	Email  string
}

func exampleConsumer(records *[]workflowstreamer.TypedRecord[testUser]) func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
	return func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		r, err := workflowstreamer.Translate[testUser](e)
		if err != nil {
			return err
		}

		*records = append(*records, *r)

		return nil
	}
}

func run(t *testing.T, ctx context.Context, store workflow.Store, workflowName string, cursorStore reflex.CursorStore, fn func(ctx context.Context, fate fate.Fate, event *reflex.Event) error, opt ...reflex.StreamOption) {
	// All tests must stop as soon as they get to the head of the stream.
	opt = append(opt, reflex.WithStreamToHead())

	spec := reflex.NewSpec(
		workflowstreamer.NewReflexStreamer(store, workflowName),
		cursorStore,
		reflex.NewConsumer("test_consumer", fn),
		opt...,
	)

	err := reflex.Run(ctx, spec)
	jtest.Require(t, reflex.ErrHeadReached, err)
}
