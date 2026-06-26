# spaniter

`spaniter` adapts Cloud Spanner row streams to Go standard iterators.

The module is deliberately lower-level than `github.com/apstndb/spanvalue`: it
does not format values, choose output formats, or own export policy. Its job is
to turn `*spanner.RowIterator` into `iter.Seq2[*spanner.Row, error]` while
preserving the Spanner iterator lifecycle.

## API

- `RowIteratorSeq`: adapts `*spanner.RowIterator` to `iter.Seq2[*spanner.Row, error]`.
- `DrainRowIterator`: consumes `*spanner.RowIterator` without yielding rows and returns metadata/stats.
- `WithResult`: captures metadata, stats, and rows read in a `RowIteratorResult`.
- `WithOnMetadata`: invokes a callback once when `ResultSetMetadata` becomes available.
- `WithOnStats`: invokes a callback after completion with query plan, query
  stats, and DML row count.
- `WithDrainOnEarlyStop`: optionally drains remaining rows after an early consumer stop so stats can be populated.
- `Rows`: adapt already-built rows for tests and virtual result sets.
- `SliceToRowSeq`: adapt existing `[]*spanner.Row` fixtures for downstream tests and fakes.

## RowIterator lifecycle

Cloud Spanner result metadata becomes available after the first `Next` call,
unless that call returns an error other than `iterator.Done`. Query plan and
query stats become available after `Next` returns `iterator.Done` only for an
iterator created with `QueryWithStats`; DML row count becomes available after
`iterator.Done`.

`RowIteratorSeq` keeps those rules explicit while leaving metadata capture
optional:

```go
rows := spaniter.RowIteratorSeq(rowIter)

for row, err := range rows {
	if err != nil {
		return err
	}
	_ = row
}
```

Once the returned sequence is invoked, `RowIteratorSeq` owns the `RowIterator`
for that run and calls `Stop` before returning. The sequence is single-use and
must not be consumed concurrently. For short call sites, code can pass a freshly
created iterator directly instead of binding it only to `defer Stop`:

```go
stmt := spanner.Statement{SQL: sql}

for row, err := range spaniter.RowIteratorSeq(txn.Query(ctx, stmt)) {
	if err != nil {
		return err
	}
	_ = row
}
```

The same ownership transfer applies when passing the sequence to a function
that immediately consumes `iter.Seq2[*spanner.Row, error]`. Merely accepting,
storing, or forwarding the sequence does not invoke it or stop the underlying
`RowIterator`. Many consumers do not need `ResultSetMetadata`; they can process
rows directly.
Use the inline form only when the called function consumes the sequence
synchronously before returning; otherwise bind the `RowIterator` and keep
responsibility for `Stop`.

```go
func consumeRows(rows iter.Seq2[*spanner.Row, error]) error {
	for row, err := range rows {
		if err != nil {
			return err
		}
		if err := processRow(row); err != nil {
			return err
		}
	}
	return nil
}

stmt := spanner.Statement{SQL: sql}

if err := consumeRows(spaniter.RowIteratorSeq(txn.Query(ctx, stmt))); err != nil {
	return err
}
```

Each yielded pair is either a non-nil row with a nil error, or a nil row with a
non-nil terminal error. After yielding a non-nil error, the sequence stops; code
should stop processing on the first error.

If the returned lazy sequence is never invoked, callers must still retain
responsibility for stopping the original `RowIterator`.

Use `WithResult` when code needs result-set metadata, stats, or rows read outside
the row loop:

```go
var result spaniter.RowIteratorResult
for row, err := range spaniter.RowIteratorSeq(rowIter, spaniter.WithResult(&result)) {
	if err != nil {
		return err
	}
	_ = row
}
_ = result.Metadata
_ = result.Stats
_ = result.RowsRead
```

Use `WithOnMetadata` or `WithOnStats` when code needs hook-style callbacks
instead of a captured result value.
For fan-in adapters or streaming sinks that must publish row-type metadata
before the first downstream row, prefer `WithOnMetadata`: `WithResult` is
updated at the same lifecycle points, but an empty result set never enters the
consumer's loop body, so loop-local polling cannot observe metadata until after
the sequence is exhausted.

If application code needs metadata or stats but does not want row values,
`DrainRowIterator` consumes the stream internally and returns only lifecycle
results:

```go
result, err := spaniter.DrainRowIterator(rowIter)
if err != nil {
	return err
}
_ = result.Metadata
_ = result.Stats
_ = result.RowsRead
```

This still reads the Spanner result stream internally because the Go client
populates stats only after `Next` returns `iterator.Done`. To avoid reading data
rows at the query level, execute a statement that returns no data rows.
If draining returns an error, the returned result can still contain partial
metadata and `RowsRead`; stats are populated only after a successful full drain.

If the consumer may stop early but the caller still needs stats, enable
`WithDrainOnEarlyStop`. This drains after any early stop, including a range-loop
`break` or an adapter that stops pulling because downstream work failed, so keep
it disabled unless that extra read work is acceptable.
It has no effect on `DrainRowIterator`, which already drains. When this option
is enabled, `RowsRead` includes rows read during the post-stop drain, including
rows not yielded to the consumer.
If the post-stop drain fails, its error cannot be yielded because the consumer
has already stopped. In that case, `WithOnStats` is not called and
`RowIteratorResult.Stats` remains zero.

```go
rows := spaniter.RowIteratorSeq(rowIter,
	spaniter.WithDrainOnEarlyStop(),
	spaniter.WithOnStats(func(got spaniter.Stats) {
		stats = got
	}),
)
```

## Example: spanvalue/writer

Applications that already expose rows as `iter.Seq2` can compose `spaniter`
with `spanvalue/writer`; this is caller-side composition, not a dependency of
`spaniter`. When `spanvalue/writer` is the only consumer of a raw
`*spanner.RowIterator`, its `WriteRowIterator` helper is the simpler direct
path. The composition below is useful when the surrounding pipeline is already
expressed as `iter.Seq2`.

`RunRowSeqDeferredMetadata` evaluates its metadata function after the first
pull, or after an empty sequence ends, so `WithResult` has already recorded the
metadata. `RowIteratorHooksFromWriter` then registers the row type and flushes
after a successful run; for a delimited writer with headers, this permits
header-only output for an empty result set. This call synchronously pulls the
sequence before returning, so it can take lifecycle ownership of the inline
`RowIteratorSeq` argument.

```go
stmt := spanner.Statement{SQL: sql}

var spannerResult spaniter.RowIteratorResult
if _, err := writer.RunRowSeqDeferredMetadata(
	func() *sppb.ResultSetMetadata { return spannerResult.Metadata },
	spaniter.RowIteratorSeq(
		txn.Query(ctx, stmt),
		spaniter.WithResult(&spannerResult),
	),
	writer.RowIteratorHooksFromWriter(w),
); err != nil {
	return err
}
```

When metadata is already available and code needs hook-level control,
`RunRowSeq` is the direct consumer form:

```go
if _, err := writer.RunRowSeq(
	metadata,
	spaniter.Rows(row1, row2),
	writer.RowIteratorHooksFromWriter(w),
); err != nil {
	return err
}
```

For ordinary writer use, `WriteRowSeq` wraps the same hook setup:

```go
if _, err := writer.WriteRowSeq(
	metadata,
	spaniter.Rows(row1, row2),
	w,
); err != nil {
	return err
}
```

This keeps `spaniter` independent of formatting packages while letting callers
reuse the standard iterator stream directly. In this sequence-based
composition, the `writer.RowIteratorResult` returned by `RunRowSeq`,
`RunRowSeqDeferredMetadata`, or `WriteRowSeq` has zero `Stats`; use
`spaniter.WithResult` or `WithOnStats` for Spanner-specific execution data. The
writer result's `RowsRead` counts successful writes, whereas
`spaniter.RowIteratorResult.RowsRead` counts rows consumed from the Spanner
iterator.

## Development

```bash
go test ./...
```
