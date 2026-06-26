package spaniter

import (
	"errors"
	"fmt"
	"iter"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/structpb"
)

// ErrNilRowIterator reports that [RowIteratorSeq] was given a nil iterator.
//
// Because [RowIteratorSeq] returns an [iter.Seq2], the error is yielded when the
// sequence is consumed rather than returned by the constructor.
var ErrNilRowIterator = errors.New("nil row iterator")

// ErrNilRow reports that an adapted source produced a nil row with a nil error.
var ErrNilRow = errors.New("nil row")

// Stats holds execution information populated on a
// [cloud.google.com/go/spanner.RowIterator] after the iterator reaches
// [iterator.Done].
//
// QueryPlan and QueryStats are set when the query used QueryWithStats. RowCount
// holds the DML row count after iterator.Done. Values are not deep-copied from
// the underlying RowIterator; treat returned maps and protos as read-only.
//
// Stats mirrors the public fields exposed by
// [cloud.google.com/go/spanner.RowIterator]. Use [Stats.ResultSetStats] when
// downstream code needs the protobuf ResultSetStats shape. The Go client has
// already decoded query stats to a map and exposes row count as a single int64,
// so Stats cannot distinguish an absent row count from row_count_exact:0. The
// Go client's PartitionedUpdate APIs return counts directly rather than through
// RowIterator, so partitioned DML counts are outside this type's normal scope.
type Stats struct {
	QueryPlan  *sppb.QueryPlan
	QueryStats map[string]any
	RowCount   int64
}

// HasResultSetStats reports whether s has fields that [Stats.ResultSetStats]
// would encode.
//
// Deprecated: call [Stats.ResultSetStats] and check whether the returned message
// is nil instead.
//
// This is useful when callers build an enclosing ResultSet and want to omit the
// stats field when the default conversion would be empty. It returns false for
// DML row_count_exact:0 because Stats cannot distinguish exact zero from an
// absent row count. Callers that know the Stats came from standard DML and need
// row_count_exact:0 should call [Stats.ResultSetStatsForDML] directly.
func (s Stats) HasResultSetStats() bool {
	return s.QueryPlan != nil || s.QueryStats != nil || s.RowCount != 0
}

// ResultSetStats returns s in Cloud Spanner protobuf ResultSetStats form.
//
// Most callers should use this method. The row-count caveat below matters only
// to consumers that distinguish an absent row_count from row_count_exact:0; code
// that treats an absent row_count as the zero value sees the same count either
// way.
//
// QueryPlan is reused, QueryStats is re-encoded as a protobuf Struct, and a
// non-zero RowCount is encoded as row_count_exact. RowIterator exposes row count
// as a plain int64, so an absent row count and an exact zero row count are
// indistinguishable; this method omits row count when RowCount is zero. Callers
// that know RowCount is a standard DML count can use
// [Stats.ResultSetStatsForDML].
//
// When s has no fields to encode, ResultSetStats returns nil, nil so callers can
// omit the stats field from an enclosing ResultSet without a separate presence
// check.
//
// ResultSetStats returns an error if QueryStats contains a key or value that
// cannot be represented by structpb.Struct.
func (s Stats) ResultSetStats() (*sppb.ResultSetStats, error) {
	return s.resultSetStats(s.RowCount != 0)
}

// ResultSetStatsForDML returns s as ResultSetStats for standard DML and always
// encodes RowCount as row_count_exact, including zero.
//
// This method is for consumers that need protobuf oneof presence to distinguish
// DML row_count_exact:0 from an absent row_count. If the consumer treats an
// absent row_count as the zero value, [Stats.ResultSetStats] is sufficient.
//
// Use this method only when the caller knows the Stats came from standard DML.
// Using it for query stats creates a misleading row_count_exact field even
// though the Spanner API would omit row_count for queries. Using
// [Stats.ResultSetStats] for DML is fine for non-zero row counts, but loses the
// distinction between absent row count and row_count_exact:0. Partitioned DML is
// outside spaniter's RowIterator path because the Go client exposes it through
// Client.PartitionedUpdate, not RowIterator.
//
// ResultSetStatsForDML returns an error if QueryStats contains a key or
// value that cannot be represented by structpb.Struct.
func (s Stats) ResultSetStatsForDML() (*sppb.ResultSetStats, error) {
	return s.resultSetStats(true)
}

func (s Stats) resultSetStats(includeExactRowCount bool) (*sppb.ResultSetStats, error) {
	if !includeExactRowCount && s.QueryPlan == nil && s.QueryStats == nil {
		return nil, nil
	}

	var queryStats *structpb.Struct
	if s.QueryStats != nil {
		var err error
		queryStats, err = structpb.NewStruct(s.QueryStats)
		if err != nil {
			return nil, fmt.Errorf("encode query stats: %w", err)
		}
	}

	stats := &sppb.ResultSetStats{
		QueryPlan:  s.QueryPlan,
		QueryStats: queryStats,
	}
	if includeExactRowCount {
		stats.RowCount = &sppb.ResultSetStats_RowCountExact{RowCountExact: s.RowCount}
	}
	return stats, nil
}

// RowIteratorResult is the metadata and stats available from a
// [cloud.google.com/go/spanner.RowIterator].
//
// RowsRead counts rows consumed from the iterator. Metadata and Stats values
// are not deep-copied from the underlying RowIterator; treat returned maps and
// protos as read-only.
type RowIteratorResult struct {
	Metadata *sppb.ResultSetMetadata
	Stats    Stats
	RowsRead int64
}

// Option configures [RowIteratorSeq] and [DrainRowIterator].
type Option func(*config)

type config struct {
	drainOnEarlyStop bool
	result           *RowIteratorResult
	onMetadata       []func(*sppb.ResultSetMetadata)
	onStats          []func(Stats)
}

func newConfig(opts []Option) config {
	cfg := config{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func resetResult(cfg config) {
	if cfg.result != nil {
		*cfg.result = RowIteratorResult{}
	}
}

// WithDrainOnEarlyStop configures [RowIteratorSeq] to consume the remaining
// rows after the consumer stops early.
//
// Draining is disabled by default to preserve normal iterator early-exit
// behavior. Use this option when callers need [WithOnStats] to run after any
// early stop, including a range-loop break or an adapter that stops pulling
// because a downstream operation failed. Errors encountered only during this
// post-stop drain cannot be yielded to the caller and therefore suppress the
// stats hook. It has no effect on [DrainRowIterator], which always drains.
func WithDrainOnEarlyStop() Option {
	return func(cfg *config) {
		cfg.drainOnEarlyStop = true
	}
}

// WithResult stores iterator lifecycle data in result as it becomes available.
//
// The pointed value is reset when iteration starts. Metadata is set before the
// first row is yielded, RowsRead is updated after each consumed row, and Stats
// is set only after the iterator reaches [iterator.Done]. On errors, result
// contains the partial lifecycle data observed before the error. A nil result is
// ignored.
func WithResult(result *RowIteratorResult) Option {
	return func(cfg *config) {
		if result != nil {
			cfg.result = result
		}
	}
}

// WithOnMetadata registers a hook that runs once when result metadata becomes
// available.
//
// For a query with rows, the hook runs after the first successful Next call and
// before that first row is yielded, so metadata captured by the hook is visible
// inside the first loop body. For an empty result set, the hook runs after
// Next returns iterator.Done. A nil hook is ignored.
func WithOnMetadata(f func(*sppb.ResultSetMetadata)) Option {
	return func(cfg *config) {
		if f != nil {
			cfg.onMetadata = append(cfg.onMetadata, f)
		}
	}
}

// WithOnStats registers a hook that runs after the adapted iterator has reached
// [iterator.Done] and has been stopped.
//
// If the consumer stops early, stats are available only when
// [WithDrainOnEarlyStop] is also configured. A nil hook is ignored.
func WithOnStats(f func(Stats)) Option {
	return func(cfg *config) {
		if f != nil {
			cfg.onStats = append(cfg.onStats, f)
		}
	}
}

// RowIteratorSeq adapts a [cloud.google.com/go/spanner.RowIterator] to a Go
// standard iterator.
//
// The returned sequence owns rowIter: once iteration starts it always calls
// [*cloud.google.com/go/spanner.RowIterator.Stop] before returning. Metadata and
// stats are exposed through [WithOnMetadata] and [WithOnStats] hooks instead of
// requiring callers to keep reading fields from the stopped RowIterator. The
// sequence is single-use and not safe for concurrent consumption; construct a
// new RowIterator for another pass.
//
// If the returned sequence is never invoked, RowIteratorSeq cannot call Stop.
// After constructing a sequence, callers must either consume it, pass it to code
// that will consume or stop it, or retain responsibility for stopping the
// original RowIterator.
//
// Each yielded pair is either a non-nil row with a nil error, or a nil row with
// a non-nil terminal error. After yielding a non-nil error, the sequence stops.
// Consumers should stop processing and return or break on the first non-nil
// error. On terminal errors, [WithResult] contains only lifecycle data observed
// before the error, and [WithOnStats] is not called.
func RowIteratorSeq(rowIter *spanner.RowIterator, opts ...Option) iter.Seq2[*spanner.Row, error] {
	if rowIter == nil {
		cfg := newConfig(opts)
		return func(yield func(*spanner.Row, error) bool) {
			resetResult(cfg)
			yield(nil, ErrNilRowIterator)
		}
	}
	return rowSourceSeq(spannerRowSource{rowIter}, opts...)
}

// DrainRowIterator consumes rowIter to [iterator.Done] without yielding rows.
//
// The helper owns rowIter and always calls
// [*cloud.google.com/go/spanner.RowIterator.Stop] before returning. It is useful
// when callers need result metadata, query stats, query plan, or DML row count
// but do not want to expose row values to application code.
// If iteration fails, the returned result can be non-nil and contain partial
// metadata and RowsRead observed before the error; stats are only populated
// after a successful drain to [iterator.Done].
//
// Cloud Spanner only populates metadata after the first Next call, and stats
// after Next returns iterator.Done. DrainRowIterator therefore still consumes
// the result stream internally; it does not ask Spanner for stats without
// reading the stream. To avoid reading data rows at the query level, callers
// must execute a statement that returns no data rows.
func DrainRowIterator(rowIter *spanner.RowIterator, opts ...Option) (*RowIteratorResult, error) {
	if rowIter == nil {
		resetResult(newConfig(opts))
		return nil, ErrNilRowIterator
	}
	return drainRowSource(spannerRowSource{rowIter}, opts...)
}

type rowSource interface {
	next() (*spanner.Row, error)
	stop()
	metadata() *sppb.ResultSetMetadata
	stats() Stats
}

type spannerRowSource struct {
	*spanner.RowIterator
}

func (s spannerRowSource) next() (*spanner.Row, error) {
	return s.Next()
}

func (s spannerRowSource) stop() {
	s.Stop()
}

func (s spannerRowSource) metadata() *sppb.ResultSetMetadata {
	return s.Metadata
}

func (s spannerRowSource) stats() Stats {
	return Stats{
		QueryPlan:  s.QueryPlan,
		QueryStats: s.QueryStats,
		RowCount:   s.RowCount,
	}
}

func rowSourceSeq(src rowSource, opts ...Option) iter.Seq2[*spanner.Row, error] {
	cfg := newConfig(opts)
	if src == nil {
		return func(yield func(*spanner.Row, error) bool) {
			resetResult(cfg)
			yield(nil, ErrNilRowIterator)
		}
	}
	return func(yield func(*spanner.Row, error) bool) {
		resetResult(cfg)

		stopped := false
		stopOnce := func() {
			if !stopped {
				stopped = true
				src.stop()
			}
		}
		defer stopOnce()

		metadataSent := false
		sendMetadata := func() {
			if metadataSent {
				return
			}
			metadataSent = true
			md := src.metadata()
			if cfg.result != nil {
				cfg.result.Metadata = md
			}
			for _, f := range cfg.onMetadata {
				f(md)
			}
		}
		sendStats := func() {
			stats := src.stats()
			if cfg.result != nil {
				cfg.result.Stats = stats
			}
			for _, f := range cfg.onStats {
				f(stats)
			}
		}

		var rowsRead int64
		updateRowsRead := func() {
			if cfg.result != nil {
				cfg.result.RowsRead = rowsRead
			}
		}

		completed := false
		for {
			row, err := src.next()
			if err != nil {
				if errors.Is(err, iterator.Done) {
					sendMetadata()
					completed = true
					break
				}
				yield(nil, err)
				return
			}
			if row == nil {
				yield(nil, ErrNilRow)
				return
			}
			sendMetadata()
			rowsRead++
			updateRowsRead()
			if !yield(row, nil) {
				if cfg.drainOnEarlyStop {
					var drained int64
					drained, completed = drain(src)
					rowsRead += drained
					updateRowsRead()
				}
				if completed {
					stopOnce()
					sendStats()
				}
				return
			}
		}

		stopOnce()
		if completed {
			sendStats()
		}
	}
}

func drain(src rowSource) (int64, bool) {
	var rowsRead int64
	for {
		row, err := src.next()
		if errors.Is(err, iterator.Done) {
			return rowsRead, true
		}
		if err != nil || row == nil {
			return rowsRead, false
		}
		rowsRead++
	}
}

func drainRowSource(src rowSource, opts ...Option) (*RowIteratorResult, error) {
	if src == nil {
		return nil, ErrNilRowIterator
	}
	cfg := newConfig(opts)
	resetResult(cfg)

	stopped := false
	stopOnce := func() {
		if !stopped {
			stopped = true
			src.stop()
		}
	}
	defer stopOnce()

	var rowsRead int64
	var metadata *sppb.ResultSetMetadata
	var metadataSent bool
	outcome := func(includeStats bool) *RowIteratorResult {
		result := &RowIteratorResult{RowsRead: rowsRead}
		if metadataSent {
			result.Metadata = metadata
		}
		if includeStats {
			result.Stats = src.stats()
		}
		return result
	}
	captureResult := func(result *RowIteratorResult) {
		if cfg.result != nil {
			*cfg.result = *result
		}
	}
	abort := func(err error) (*RowIteratorResult, error) {
		stopOnce()
		result := outcome(false)
		captureResult(result)
		return result, err
	}

	sendMetadata := func() {
		if metadataSent {
			return
		}
		metadataSent = true
		metadata = src.metadata()
		if cfg.result != nil {
			cfg.result.Metadata = metadata
		}
		for _, f := range cfg.onMetadata {
			f(metadata)
		}
	}
	sendStats := func(stats Stats) {
		if cfg.result != nil {
			cfg.result.Stats = stats
		}
		for _, f := range cfg.onStats {
			f(stats)
		}
	}

	for {
		row, err := src.next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				sendMetadata()
				stopOnce()
				result := outcome(true)
				captureResult(result)
				sendStats(result.Stats)
				return result, nil
			}
			return abort(err)
		}
		if row == nil {
			return abort(ErrNilRow)
		}
		sendMetadata()
		rowsRead++
		if cfg.result != nil {
			cfg.result.RowsRead = rowsRead
		}
	}
}
