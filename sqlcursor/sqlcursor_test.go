package sqlcursor_test

import (
	"context"
	"github.com/andrewwormald/workflow/sqlcursor"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSQLCursor_Set_Get(t *testing.T) {
	dbc := ConnectForTesting(t)
	cursor := sqlcursor.New("workflow_cursors", dbc, dbc)

	ctx := context.TODO()
	key := "my_key"
	expectedValue := "my_value"

	err := cursor.Set(ctx, key, expectedValue)
	jtest.RequireNil(t, err)

	actual, err := cursor.Get(ctx, key)
	jtest.RequireNil(t, err)
	require.Equal(t, expectedValue, actual)

	expectedValue = "my_new_value"

	err = cursor.Set(ctx, key, expectedValue)
	jtest.RequireNil(t, err)

	actual, err = cursor.Get(ctx, key)
	jtest.RequireNil(t, err)
	require.Equal(t, expectedValue, actual)
}
