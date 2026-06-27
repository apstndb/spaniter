package spaniter

import (
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

func iteratorResult(stats Stats, enc StatsEncoding) RowIteratorResult {
	return RowIteratorResult{Stats: stats, statsEnc: enc, statsCaptured: true}
}

func TestRowIteratorResult_StatsProto(t *testing.T) {
	t.Parallel()

	t.Run("default omits zero row count", func(t *testing.T) {
		t.Parallel()
		got, err := iteratorResult(Stats{RowCount: 0}, StatsEncodingDefault).StatsProto()
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("StatsProto = %v, want nil", got)
		}
	})

	t.Run("DML exact includes zero row count", func(t *testing.T) {
		t.Parallel()
		got, err := iteratorResult(Stats{RowCount: 0}, StatsEncodingDMLExact).StatsProto()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := got.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
			t.Fatalf("RowCount type = %T, want RowCountExact", got.GetRowCount())
		}
	})

	t.Run("query plan and non-zero row count", func(t *testing.T) {
		t.Parallel()
		got, err := iteratorResult(Stats{
			QueryPlan:  &sppb.QueryPlan{},
			QueryStats: realisticQueryStats(),
			RowCount:   2,
		}, StatsEncodingDefault).StatsProto()
		if err != nil {
			t.Fatal(err)
		}
		if got.GetQueryPlan() == nil {
			t.Fatal("QueryPlan = nil, want non-nil")
		}
		if got.GetQueryStats() == nil {
			t.Fatal("QueryStats = nil, want non-nil")
		}
		if _, ok := got.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
			t.Fatalf("RowCount type = %T, want RowCountExact", got.GetRowCount())
		}
		if got.GetRowCountExact() != 2 {
			t.Fatalf("RowCountExact = %d, want 2", got.GetRowCountExact())
		}
	})

	t.Run("nil vs empty query stats", func(t *testing.T) {
		t.Parallel()
		gotNil, err := iteratorResult(Stats{}, StatsEncodingDefault).StatsProto()
		if err != nil {
			t.Fatal(err)
		}
		if gotNil != nil {
			t.Fatalf("StatsProto = %v, want nil", gotNil)
		}

		gotEmpty, err := iteratorResult(Stats{QueryStats: map[string]any{}}, StatsEncodingDefault).StatsProto()
		if err != nil {
			t.Fatal(err)
		}
		if gotEmpty == nil || gotEmpty.GetQueryStats() == nil {
			t.Fatalf("StatsProto = %v, want present empty query_stats", gotEmpty)
		}
	})

	t.Run("query stats encoding error", func(t *testing.T) {
		t.Parallel()
		_, err := iteratorResult(Stats{
			QueryStats: map[string]any{"unsupported": func() {}},
		}, StatsEncodingDefault).StatsProto()
		if err == nil {
			t.Fatal("error = nil, want query stats encoding error")
		}
		if !strings.Contains(err.Error(), "encode query stats") {
			t.Fatalf("error = %v, want encode query stats failure", err)
		}
	})

	t.Run("mixed query stats", func(t *testing.T) {
		t.Parallel()
		got, err := iteratorResult(Stats{QueryStats: map[string]any{
			"null_value": nil,
			"bool_value": true,
			"number":     1.5,
			"object":     map[string]any{"nested": "value"},
			"array":      []any{"x", float64(2), false},
		}}, StatsEncodingDefault).StatsProto()
		if err != nil {
			t.Fatal(err)
		}
		if got.QueryStats == nil {
			t.Fatal("QueryStats = nil, want struct")
		}
		asMap := got.QueryStats.AsMap()
		if asMap["bool_value"] != true {
			t.Fatalf("bool_value = %v, want true", asMap["bool_value"])
		}
	})
}

func TestRowIteratorResult_ResultSet(t *testing.T) {
	t.Parallel()

	md := &sppb.ResultSetMetadata{}
	got, err := RowIteratorResult{
		Metadata:      md,
		Stats:         Stats{RowCount: 0},
		statsEnc:      StatsEncodingDMLExact,
		statsCaptured: true,
	}.ResultSet(nil)
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

func TestWithStatsEncodingOnDrain(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row := mustNewRow(t, []string{"id"}, []any{int64(1)})
	src := &stubRowSource{
		rows:      []*spanner.Row{row},
		md:        md,
		wantStats: Stats{RowCount: 0},
	}
	var result RowIteratorResult
	got, err := drainRowSource(src, WithResult(&result), WithStatsEncoding(StatsEncodingDMLExact))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []*RowIteratorResult{&result, got} {
		stats, err := r.StatsProto()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := stats.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
			t.Fatalf("RowCount type = %T, want RowCountExact", stats.GetRowCount())
		}
	}
}

func TestDrainRowSource_directReturnPreservesStatsEncoding(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	src := &stubRowSource{
		md:        md,
		wantStats: Stats{RowCount: 0},
	}
	got, err := drainRowSource(src, WithStatsEncoding(StatsEncodingDMLExact))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := got.StatsProto()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stats.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
		t.Fatalf("RowCount type = %T, want RowCountExact", stats.GetRowCount())
	}
}

func TestRowIteratorResult_StatsProtoOmitsUncapturedDMLExact(t *testing.T) {
	t.Parallel()

	got, err := RowIteratorResult{
		statsEnc: StatsEncodingDMLExact,
	}.StatsProto()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("StatsProto = %v, want nil for uncaptured stats", got)
	}
}

func TestRowIteratorSeqWithStatsEncodingDMLExact(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row := mustNewRow(t, []string{"id"}, []any{int64(1)})
	src := &stubRowSource{
		rows:      []*spanner.Row{row},
		md:        md,
		wantStats: Stats{RowCount: 0},
	}
	var result RowIteratorResult
	for row, err := range rowSourceSeq(src, WithResult(&result), WithStatsEncoding(StatsEncodingDMLExact)) {
		if err != nil {
			t.Fatal(err)
		}
		_ = row
	}
	got, err := result.StatsProto()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
		t.Fatalf("RowCount type = %T, want RowCountExact", got.GetRowCount())
	}
}

func TestRowIteratorSeqWithStatsEncodingAndEarlyStopDrain(t *testing.T) {
	t.Parallel()

	rows := []*spanner.Row{
		mustNewRow(t, []string{"id"}, []any{int64(1)}),
		mustNewRow(t, []string{"id"}, []any{int64(2)}),
	}
	src := &stubRowSource{
		rows:      rows,
		md:        metadataWithColumnNames("id"),
		wantStats: Stats{RowCount: 0},
	}
	var result RowIteratorResult
	seq := rowSourceSeq(src, WithResult(&result), WithStatsEncoding(StatsEncodingDMLExact), WithDrainOnEarlyStop())
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		_ = row
		break
	}
	got, err := result.StatsProto()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
		t.Fatalf("RowCount type = %T, want RowCountExact after early-stop drain", got.GetRowCount())
	}
}

func TestDrainRowSource_midStreamErrorOmitsFabricatedDMLStats(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stream failed")
	src := &stubRowSource{
		rows:  []*spanner.Row{mustNewRow(t, []string{"id"}, []any{int64(1)})},
		md:    metadataWithColumnNames("id"),
		errAt: 1,
		err:   wantErr,
	}
	got, err := drainRowSource(src, WithStatsEncoding(StatsEncodingDMLExact))
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	stats, err := got.StatsProto()
	if err != nil {
		t.Fatal(err)
	}
	if stats != nil {
		t.Fatalf("StatsProto = %v, want nil on partial drain", stats)
	}
}
