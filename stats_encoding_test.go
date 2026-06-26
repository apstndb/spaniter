package spaniter

import (
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

func TestStats_ResultSetStatsEncoded(t *testing.T) {
	t.Parallel()

	t.Run("default omits zero row count", func(t *testing.T) {
		t.Parallel()
		got, err := (Stats{RowCount: 0}).ResultSetStatsEncoded(StatsEncodingDefault)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("ResultSetStatsEncoded = %v, want nil", got)
		}
	})

	t.Run("DML exact includes zero row count", func(t *testing.T) {
		t.Parallel()
		got, err := (Stats{RowCount: 0}).ResultSetStatsEncoded(StatsEncodingDMLExact)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := got.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
			t.Fatalf("RowCount type = %T, want RowCountExact", got.GetRowCount())
		}
	})
}

func TestRowIteratorResult_ResultSet(t *testing.T) {
	t.Parallel()

	md := &sppb.ResultSetMetadata{}
	got, err := (RowIteratorResult{
		Metadata: md,
		Stats:    Stats{RowCount: 0},
	}).ResultSet(nil, StatsEncodingDMLExact)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata not preserved")
	}
	if got.Stats == nil {
		t.Fatal("Stats = nil, want DML zero row count")
	}
}
