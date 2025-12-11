package traceql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLToFetchSpansRequestMock(t *testing.T) {
	req, err := SQLToFetchSpansRequest("SELECT * FROM spans")
	require.NoError(t, err)
	fmt.Println(req)
}
