package spaniter

import (
	"errors"
	"iter"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var _ rowSource = (*stubRowSource)(nil)

type stubRowSource struct {
	rows      []*spanner.Row
	md        *sppb.ResultSetMetadata
	wantStats Stats
	errAt     int
	err       error

	nextCalls int
	done      bool
	stopped   bool
}

func (s *stubRowSource) next() (*spanner.Row, error) {
	if s.err != nil && s.nextCalls == s.errAt {
		s.nextCalls++
		return nil, s.err
	}
	if s.nextCalls < len(s.rows) {
		row := s.rows[s.nextCalls]
		s.nextCalls++
		return row, nil
	}
	s.nextCalls++
	s.done = true
	return nil, iterator.Done
}

func (s *stubRowSource) stop() {
	s.stopped = true
}

func (s *stubRowSource) metadata() *sppb.ResultSetMetadata {
	if s.nextCalls == 0 {
		return nil
	}
	return s.md
}

func (s *stubRowSource) stats() Stats {
	if !s.done || !s.stopped {
		return Stats{}
	}
	return s.wantStats
}

func TestRowIteratorSeq_nilIterator(t *testing.T) {
	t.Parallel()

	var gotErr error
	for _, err := range RowIteratorSeq(nil) {
		gotErr = err
	}
	if !errors.Is(gotErr, ErrNilRowIterator) {
		t.Fatalf("error = %v, want ErrNilRowIterator", gotErr)
	}
}

func TestRowIteratorSeq_nilIteratorWithResultResets(t *testing.T) {
	t.Parallel()

	result := RowIteratorResult{
		Metadata: metadataWithColumnNames("stale"),
		Stats:    Stats{RowCount: 99, QueryStats: realisticQueryStats()},
		RowsRead: 99,
	}

	var gotErr error
	for _, err := range RowIteratorSeq(nil, WithResult(&result)) {
		gotErr = err
	}
	if !errors.Is(gotErr, ErrNilRowIterator) {
		t.Fatalf("error = %v, want ErrNilRowIterator", gotErr)
	}
	if result.Metadata != nil {
		t.Fatal("metadata was not reset")
	}
	if result.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", result.RowsRead)
	}
	requireZeroStats(t, result.Stats)
}

func TestDrainRowIterator_nilIterator(t *testing.T) {
	t.Parallel()

	_, err := DrainRowIterator(nil)
	if !errors.Is(err, ErrNilRowIterator) {
		t.Fatalf("error = %v, want ErrNilRowIterator", err)
	}
}

func TestDrainRowIterator_nilIteratorWithResultResets(t *testing.T) {
	t.Parallel()

	result := RowIteratorResult{
		Metadata: metadataWithColumnNames("stale"),
		Stats:    Stats{RowCount: 99, QueryStats: realisticQueryStats()},
		RowsRead: 99,
	}

	_, err := DrainRowIterator(nil, WithResult(&result))
	if !errors.Is(err, ErrNilRowIterator) {
		t.Fatalf("error = %v, want ErrNilRowIterator", err)
	}
	if result.Metadata != nil {
		t.Fatal("metadata was not reset")
	}
	if result.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", result.RowsRead)
	}
	requireZeroStats(t, result.Stats)
}

func TestStats_ResultSetStats(t *testing.T) {
	t.Parallel()

	plan := &sppb.QueryPlan{}
	queryStats := realisticQueryStats()
	wantQueryStats, err := structpb.NewStruct(queryStats)
	if err != nil {
		t.Fatal(err)
	}
	got, err := (Stats{
		QueryPlan:  plan,
		QueryStats: queryStats,
		RowCount:   2,
	}).ResultSetStats()
	if err != nil {
		t.Fatal(err)
	}
	if got.QueryPlan != plan {
		t.Fatal("QueryPlan was not preserved")
	}
	if !proto.Equal(got.QueryStats, wantQueryStats) {
		t.Fatalf("QueryStats = %v, want %v", got.QueryStats, wantQueryStats)
	}
	if _, ok := got.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
		t.Fatalf("RowCount type = %T, want RowCountExact", got.GetRowCount())
	}
	if got.GetRowCountExact() != 2 {
		t.Fatalf("RowCountExact = %d, want 2", got.GetRowCountExact())
	}
}

func TestStats_IsZero(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		stats Stats
		want  bool
	}{
		{
			name: "empty",
			want: true,
		},
		{
			name:  "query plan",
			stats: Stats{QueryPlan: &sppb.QueryPlan{}},
		},
		{
			name:  "nil query stats absent",
			stats: Stats{QueryStats: nil},
			want:  true,
		},
		{
			name:  "empty query stats present",
			stats: Stats{QueryStats: map[string]any{}},
		},
		{
			name:  "non-zero row count",
			stats: Stats{RowCount: 1},
		},
		{
			name:  "zero row count",
			stats: Stats{RowCount: 0},
			want:  true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.stats.IsZero(); got != tt.want {
				t.Fatalf("IsZero() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestStats_ResultSetStatsQueryStatsPresence(t *testing.T) {
	t.Parallel()

	gotNil, err := (Stats{}).ResultSetStats()
	if err != nil {
		t.Fatal(err)
	}
	if gotNil.QueryStats != nil {
		t.Fatalf("QueryStats = %v, want nil", gotNil.QueryStats)
	}

	gotEmpty, err := (Stats{QueryStats: map[string]any{}}).ResultSetStats()
	if err != nil {
		t.Fatal(err)
	}
	if gotEmpty.QueryStats == nil {
		t.Fatal("QueryStats = nil, want empty struct")
	}
	if len(gotEmpty.QueryStats.Fields) != 0 {
		t.Fatalf("QueryStats fields = %v, want empty", gotEmpty.QueryStats.Fields)
	}
}

func TestStats_ResultSetStatsOmitsZeroRowCount(t *testing.T) {
	t.Parallel()

	got, err := (Stats{RowCount: 0}).ResultSetStats()
	if err != nil {
		t.Fatal(err)
	}
	if got.GetRowCount() != nil {
		t.Fatalf("RowCount = %T, want nil", got.GetRowCount())
	}
}

func TestStats_ResultSetStatsForDMLIncludesZero(t *testing.T) {
	t.Parallel()

	got, err := (Stats{RowCount: 0}).ResultSetStatsForDML()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.GetRowCount().(*sppb.ResultSetStats_RowCountExact); !ok {
		t.Fatalf("RowCount type = %T, want RowCountExact", got.GetRowCount())
	}
	if got.GetRowCountExact() != 0 {
		t.Fatalf("RowCountExact = %d, want 0", got.GetRowCountExact())
	}
}

func TestStats_ResultSetStatsQueryStatsError(t *testing.T) {
	t.Parallel()

	_, err := (Stats{
		QueryStats: map[string]any{"unsupported": func() {}},
	}).ResultSetStats()
	if err == nil {
		t.Fatal("error = nil, want query stats encoding error")
	}
	if !strings.Contains(err.Error(), "encode query stats") {
		t.Fatalf("error = %v, want encode query stats context", err)
	}
}

func TestRowSourceSeq_metadataBeforeFirstYield(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row := mustNewRow(t, []string{"id"}, []any{int64(1)})
	src := &stubRowSource{rows: []*spanner.Row{row}, md: md}

	var gotMD *sppb.ResultSetMetadata
	var sawFirstRow bool
	for gotRow, err := range rowSourceSeq(src, WithOnMetadata(func(md *sppb.ResultSetMetadata) {
		gotMD = md
	})) {
		if err != nil {
			t.Fatal(err)
		}
		if gotMD != md {
			t.Fatal("metadata hook did not run before first yield")
		}
		if gotRow != row {
			t.Fatal("unexpected row")
		}
		sawFirstRow = true
		break
	}
	if !sawFirstRow {
		t.Fatal("sequence yielded no rows")
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestRowSourceSeq_completeCallsStats(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row := mustNewRow(t, []string{"id"}, []any{int64(1)})
	wantStats := Stats{RowCount: 1, QueryStats: realisticQueryStats()}
	src := &stubRowSource{rows: []*spanner.Row{row}, md: md, wantStats: wantStats}

	var gotStats Stats
	rows := 0
	for _, err := range rowSourceSeq(src, WithOnStats(func(stats Stats) {
		gotStats = stats
	})) {
		if err != nil {
			t.Fatal(err)
		}
		rows++
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
	if gotStats.RowCount != wantStats.RowCount {
		t.Fatalf("RowCount = %d, want %d", gotStats.RowCount, wantStats.RowCount)
	}
	if gotStats.QueryStats["elapsed_time"] != wantStats.QueryStats["elapsed_time"] {
		t.Fatalf("QueryStats = %v, want elapsed_time=%q", gotStats.QueryStats, wantStats.QueryStats["elapsed_time"])
	}
}

func TestRowSourceSeq_withResultComplete(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	rows := []*spanner.Row{
		mustNewRow(t, []string{"id"}, []any{int64(1)}),
		mustNewRow(t, []string{"id"}, []any{int64(2)}),
	}
	wantStats := Stats{RowCount: 2, QueryStats: realisticQueryStats()}
	src := &stubRowSource{rows: rows, md: md, wantStats: wantStats}

	result := RowIteratorResult{RowsRead: 99, Stats: Stats{RowCount: 99}}
	var statsHookSawCaptured bool
	var sawFirstRow bool
	for _, err := range rowSourceSeq(src,
		WithResult(&result),
		WithOnStats(func(stats Stats) {
			statsHookSawCaptured = result.Stats.RowCount == stats.RowCount
		}),
	) {
		if err != nil {
			t.Fatal(err)
		}
		if !sawFirstRow {
			sawFirstRow = true
			if result.Metadata != md {
				t.Fatal("metadata was not captured before first yield")
			}
			if result.RowsRead != 1 {
				t.Fatalf("RowsRead at first yield = %d, want 1", result.RowsRead)
			}
		}
	}
	if result.Metadata != md {
		t.Fatal("metadata not captured")
	}
	if result.RowsRead != 2 {
		t.Fatalf("RowsRead = %d, want 2", result.RowsRead)
	}
	if result.Stats.RowCount != 2 {
		t.Fatalf("RowCount = %d, want 2", result.Stats.RowCount)
	}
	if result.Stats.QueryStats["elapsed_time"] != wantStats.QueryStats["elapsed_time"] {
		t.Fatalf("QueryStats = %v, want elapsed_time=%q", result.Stats.QueryStats, wantStats.QueryStats["elapsed_time"])
	}
	if !statsHookSawCaptured {
		t.Fatal("WithResult stats were not visible before stats hook")
	}
}

func TestRowSourceSeq_metadataCalledOnce(t *testing.T) {
	t.Parallel()

	rows := []*spanner.Row{
		mustNewRow(t, []string{"id"}, []any{int64(1)}),
		mustNewRow(t, []string{"id"}, []any{int64(2)}),
	}
	src := &stubRowSource{rows: rows, md: metadataWithColumnNames("id")}

	var metadataCalls int
	for _, err := range rowSourceSeq(src, WithOnMetadata(func(*sppb.ResultSetMetadata) {
		metadataCalls++
	})) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if metadataCalls != 1 {
		t.Fatalf("metadataCalls = %d, want 1", metadataCalls)
	}
}

func TestRowSourceSeq_emptyCallsMetadataAndStats(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	wantStats := Stats{RowCount: 0}
	src := &stubRowSource{md: md, wantStats: wantStats}

	var gotMD *sppb.ResultSetMetadata
	var gotStats *Stats
	for _, err := range rowSourceSeq(src,
		WithOnMetadata(func(md *sppb.ResultSetMetadata) {
			gotMD = md
		}),
		WithOnStats(func(stats Stats) {
			gotStats = &stats
		}),
	) {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("empty source yielded a row")
	}
	if gotMD != md {
		t.Fatal("metadata hook did not receive metadata")
	}
	if gotStats == nil {
		t.Fatal("stats hook was not called")
	}
}

func TestDrainRowSource_discardsRowsAndReturnsMetadataAndStats(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	rows := []*spanner.Row{
		mustNewRow(t, []string{"id"}, []any{int64(1)}),
		mustNewRow(t, []string{"id"}, []any{int64(2)}),
	}
	wantStats := Stats{RowCount: 2, QueryStats: realisticQueryStats()}
	src := &stubRowSource{rows: rows, md: md, wantStats: wantStats}

	var gotMD *sppb.ResultSetMetadata
	var gotStats *Stats
	var captured RowIteratorResult
	got, err := drainRowSource(src,
		WithResult(&captured),
		WithOnMetadata(func(md *sppb.ResultSetMetadata) {
			gotMD = md
		}),
		WithOnStats(func(stats Stats) {
			gotStats = &stats
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata not returned")
	}
	if gotMD != md {
		t.Fatal("metadata hook did not receive metadata")
	}
	if got.RowsRead != 2 {
		t.Fatalf("RowsRead = %d, want 2", got.RowsRead)
	}
	if got.Stats.RowCount != 2 {
		t.Fatalf("RowCount = %d, want 2", got.Stats.RowCount)
	}
	if gotStats == nil || gotStats.RowCount != 2 {
		t.Fatalf("stats hook = %+v, want RowCount 2", gotStats)
	}
	if captured.Metadata != got.Metadata {
		t.Fatal("WithResult metadata does not match returned result")
	}
	if captured.RowsRead != got.RowsRead {
		t.Fatalf("WithResult RowsRead = %d, want %d", captured.RowsRead, got.RowsRead)
	}
	if captured.Stats.RowCount != got.Stats.RowCount {
		t.Fatalf("WithResult RowCount = %d, want %d", captured.Stats.RowCount, got.Stats.RowCount)
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestDrainRowSource_emptyReturnsMetadataAndStats(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	src := &stubRowSource{md: md, wantStats: Stats{RowCount: 0}}

	got, err := drainRowSource(src)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata not returned")
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
	}
	if got.Stats.RowCount != 0 {
		t.Fatalf("RowCount = %d, want 0", got.Stats.RowCount)
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestDrainRowSource_midStreamErrorReturnsPartialResult(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stream failed")
	src := &stubRowSource{
		rows:  []*spanner.Row{mustNewRow(t, []string{"id"}, []any{int64(1)})},
		md:    metadataWithColumnNames("id"),
		errAt: 1,
		err:   wantErr,
	}

	var metadataCalls int
	var statsCalled bool
	got, err := drainRowSource(src,
		WithOnMetadata(func(*sppb.ResultSetMetadata) {
			metadataCalls++
		}),
		WithOnStats(func(Stats) {
			statsCalled = true
		}),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got == nil {
		t.Fatal("result is nil")
	}
	if got.RowsRead != 1 {
		t.Fatalf("RowsRead = %d, want 1", got.RowsRead)
	}
	if got.Metadata != src.md {
		t.Fatal("metadata not returned")
	}
	requireZeroStats(t, got.Stats)
	if metadataCalls != 1 {
		t.Fatalf("metadataCalls = %d, want 1", metadataCalls)
	}
	if statsCalled {
		t.Fatal("stats hook should not run on stream error")
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestDrainRowSource_firstErrorSkipsHooks(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("query failed")
	src := &stubRowSource{md: metadataWithColumnNames("id"), errAt: 0, err: wantErr}

	var metadataCalled bool
	var statsCalled bool
	got, err := drainRowSource(src,
		WithOnMetadata(func(*sppb.ResultSetMetadata) {
			metadataCalled = true
		}),
		WithOnStats(func(Stats) {
			statsCalled = true
		}),
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got == nil {
		t.Fatal("result is nil")
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
	}
	if got.Metadata != nil {
		t.Fatal("metadata should not be returned before it is observed")
	}
	requireZeroStats(t, got.Stats)
	if metadataCalled {
		t.Fatal("metadata hook should not run before metadata is available")
	}
	if statsCalled {
		t.Fatal("stats hook should not run on query error")
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestDrainRowSource_nilRowYieldsError(t *testing.T) {
	t.Parallel()

	src := &stubRowSource{rows: []*spanner.Row{nil}, md: metadataWithColumnNames("id")}

	got, err := drainRowSource(src)
	if !errors.Is(err, ErrNilRow) {
		t.Fatalf("error = %v, want ErrNilRow", err)
	}
	if got == nil {
		t.Fatal("result is nil")
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
	}
	if got.Metadata != nil {
		t.Fatal("metadata should not be returned for nil row")
	}
	requireZeroStats(t, got.Stats)
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestRowSourceSeq_earlyBreakWithoutDrainSkipsStats(t *testing.T) {
	t.Parallel()

	rows := []*spanner.Row{
		mustNewRow(t, []string{"id"}, []any{int64(1)}),
		mustNewRow(t, []string{"id"}, []any{int64(2)}),
	}
	src := &stubRowSource{rows: rows, md: metadataWithColumnNames("id"), wantStats: Stats{RowCount: 2}}

	var statsCalled bool
	var result RowIteratorResult
	for _, err := range rowSourceSeq(src,
		WithResult(&result),
		WithOnStats(func(Stats) {
			statsCalled = true
		}),
	) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if statsCalled {
		t.Fatal("stats hook should not run when early break is not drained")
	}
	if src.nextCalls != 1 {
		t.Fatalf("nextCalls = %d, want 1", src.nextCalls)
	}
	if result.RowsRead != 1 {
		t.Fatalf("RowsRead = %d, want 1", result.RowsRead)
	}
	if result.Metadata != src.md {
		t.Fatal("metadata not captured")
	}
	if result.Stats.RowCount != 0 {
		t.Fatalf("RowCount = %d, want 0", result.Stats.RowCount)
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestRowSourceSeq_earlyBreakWithDrainCallsStats(t *testing.T) {
	t.Parallel()

	rows := []*spanner.Row{
		mustNewRow(t, []string{"id"}, []any{int64(1)}),
		mustNewRow(t, []string{"id"}, []any{int64(2)}),
	}
	src := &stubRowSource{rows: rows, md: metadataWithColumnNames("id"), wantStats: Stats{RowCount: 2}}

	var gotStats *Stats
	var result RowIteratorResult
	for _, err := range rowSourceSeq(src,
		WithResult(&result),
		WithDrainOnEarlyStop(),
		WithOnStats(func(stats Stats) {
			gotStats = &stats
		}),
	) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if gotStats == nil {
		t.Fatal("stats hook was not called")
	}
	if gotStats.RowCount != 2 {
		t.Fatalf("RowCount = %d, want 2", gotStats.RowCount)
	}
	if src.nextCalls != 3 {
		t.Fatalf("nextCalls = %d, want 3", src.nextCalls)
	}
	if result.RowsRead != 2 {
		t.Fatalf("RowsRead = %d, want 2", result.RowsRead)
	}
	if result.Stats.RowCount != 2 {
		t.Fatalf("captured RowCount = %d, want 2", result.Stats.RowCount)
	}
}

func TestRowSourceSeq_pullStopWithDrainDrains(t *testing.T) {
	t.Parallel()

	rows := []*spanner.Row{
		mustNewRow(t, []string{"id"}, []any{int64(1)}),
		mustNewRow(t, []string{"id"}, []any{int64(2)}),
	}
	src := &stubRowSource{rows: rows, md: metadataWithColumnNames("id"), wantStats: Stats{RowCount: 2}}

	var gotStats *Stats
	next, stop := iter.Pull2(rowSourceSeq(src,
		WithDrainOnEarlyStop(),
		WithOnStats(func(stats Stats) {
			gotStats = &stats
		}),
	))
	row, err, ok := next()
	if !ok {
		t.Fatal("first pull returned no row")
	}
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("first pull returned nil row")
	}
	stop()

	if gotStats == nil {
		t.Fatal("stats hook was not called")
	}
	if gotStats.RowCount != 2 {
		t.Fatalf("RowCount = %d, want 2", gotStats.RowCount)
	}
	if src.nextCalls != 3 {
		t.Fatalf("nextCalls = %d, want 3", src.nextCalls)
	}
}

func TestRowSourceSeq_earlyBreakWithDrainErrorSuppressesStats(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("drain failed")
	src := &stubRowSource{
		rows:  []*spanner.Row{mustNewRow(t, []string{"id"}, []any{int64(1)})},
		md:    metadataWithColumnNames("id"),
		errAt: 1,
		err:   wantErr,
	}

	var statsCalled bool
	for _, err := range rowSourceSeq(src,
		WithDrainOnEarlyStop(),
		WithOnStats(func(Stats) {
			statsCalled = true
		}),
	) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if statsCalled {
		t.Fatal("stats hook should not run when drain fails")
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestRowSourceSeq_earlyBreakWithDrainNilRowSuppressesStats(t *testing.T) {
	t.Parallel()

	rows := []*spanner.Row{
		mustNewRow(t, []string{"id"}, []any{int64(1)}),
		nil,
	}
	src := &stubRowSource{rows: rows, md: metadataWithColumnNames("id"), wantStats: Stats{RowCount: 2}}

	var statsCalled bool
	for _, err := range rowSourceSeq(src,
		WithDrainOnEarlyStop(),
		WithOnStats(func(Stats) {
			statsCalled = true
		}),
	) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if statsCalled {
		t.Fatal("stats hook should not run when drain sees a nil row")
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestRowSourceSeq_firstErrorSkipsMetadataAndStats(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("query failed")
	src := &stubRowSource{md: metadataWithColumnNames("id"), errAt: 0, err: wantErr}

	var metadataCalled bool
	var statsCalled bool
	var gotErr error
	for _, err := range rowSourceSeq(src,
		WithOnMetadata(func(*sppb.ResultSetMetadata) {
			metadataCalled = true
		}),
		WithOnStats(func(Stats) {
			statsCalled = true
		}),
	) {
		gotErr = err
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("error = %v, want %v", gotErr, wantErr)
	}
	if metadataCalled {
		t.Fatal("metadata hook should not run before metadata is available")
	}
	if statsCalled {
		t.Fatal("stats hook should not run on query error")
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestRowSourceSeq_midStreamErrorStopsWithoutStats(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stream failed")
	src := &stubRowSource{
		rows:  []*spanner.Row{mustNewRow(t, []string{"id"}, []any{int64(1)})},
		md:    metadataWithColumnNames("id"),
		errAt: 1,
		err:   wantErr,
	}

	var metadataCalls int
	var statsCalled bool
	var result RowIteratorResult
	var rows int
	var gotErr error
	for _, err := range rowSourceSeq(src,
		WithResult(&result),
		WithOnMetadata(func(*sppb.ResultSetMetadata) {
			metadataCalls++
		}),
		WithOnStats(func(Stats) {
			statsCalled = true
		}),
	) {
		if err != nil {
			gotErr = err
			continue
		}
		rows++
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("error = %v, want %v", gotErr, wantErr)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	if metadataCalls != 1 {
		t.Fatalf("metadataCalls = %d, want 1", metadataCalls)
	}
	if statsCalled {
		t.Fatal("stats hook should not run on stream error")
	}
	if result.RowsRead != 1 {
		t.Fatalf("RowsRead = %d, want 1", result.RowsRead)
	}
	if result.Metadata != src.md {
		t.Fatal("metadata not captured")
	}
	if result.Stats.RowCount != 0 {
		t.Fatalf("RowCount = %d, want 0", result.Stats.RowCount)
	}
	if !src.stopped {
		t.Fatal("source was not stopped")
	}
}

func TestRowSourceSeq_nilRowYieldsError(t *testing.T) {
	t.Parallel()

	src := &stubRowSource{rows: []*spanner.Row{nil}, md: metadataWithColumnNames("id")}

	var gotErr error
	for _, err := range rowSourceSeq(src) {
		gotErr = err
	}
	if !errors.Is(gotErr, ErrNilRow) {
		t.Fatalf("error = %v, want ErrNilRow", gotErr)
	}
}

func TestRowSourceSeq_nilOptionsAndHooksIgnored(t *testing.T) {
	t.Parallel()

	row := mustNewRow(t, []string{"id"}, []any{int64(1)})
	src := &stubRowSource{rows: []*spanner.Row{row}, md: metadataWithColumnNames("id")}

	var rows int
	for _, err := range rowSourceSeq(src, nil, WithResult(nil), WithOnMetadata(nil), WithOnStats(nil)) {
		if err != nil {
			t.Fatal(err)
		}
		rows++
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
}

func mustNewRow(t *testing.T, names []string, values []any) *spanner.Row {
	t.Helper()
	row, err := spanner.NewRow(names, values)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func metadataWithColumnNames(names ...string) *sppb.ResultSetMetadata {
	fields := make([]*sppb.StructType_Field, len(names))
	for i, name := range names {
		fields[i] = &sppb.StructType_Field{
			Name: name,
			Type: &sppb.Type{Code: sppb.TypeCode_INT64},
		}
	}
	return &sppb.ResultSetMetadata{
		RowType: &sppb.StructType{Fields: fields},
	}
}

func realisticQueryStats() map[string]any {
	// Cloud Spanner query_stats values are wire strings, even for counts,
	// booleans, bytes, and durations.
	return map[string]any{
		"bytes_returned":               "8",
		"cpu_time":                     "0.2 msecs",
		"deleted_rows_scanned":         "0",
		"elapsed_time":                 "0.23 msecs",
		"filesystem_delay_seconds":     "0 msecs",
		"is_graph_query":               "false",
		"locking_delay":                "0 msecs",
		"memory_peak_usage_bytes":      "4",
		"memory_usage_percentage":      "0.000",
		"optimizer_statistics_package": "auto_20250604_03_26_04UTC",
		"optimizer_version":            "7",
		"query_plan_cached":            "true",
		"query_text":                   "SELECT 1",
		"remote_server_calls":          "0/0",
		"rows_returned":                "1",
		"rows_scanned":                 "0",
		"runtime_cached":               "true",
		"server_queue_delay":           "0.01 msecs",
		"statistics_load_time":         "0",
		"total_memory_peak_usage_byte": "4",
	}
}

func requireZeroStats(t *testing.T, stats Stats) {
	t.Helper()
	if stats.QueryPlan != nil {
		t.Fatalf("QueryPlan = %v, want nil", stats.QueryPlan)
	}
	if stats.QueryStats != nil {
		t.Fatalf("QueryStats = %v, want nil", stats.QueryStats)
	}
	if stats.RowCount != 0 {
		t.Fatalf("RowCount = %d, want 0", stats.RowCount)
	}
}
