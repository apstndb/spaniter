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
- `WithOnMetadata`: captures `ResultSetMetadata` as soon as it is available.
- `WithOnStats`: captures query plan, query stats, and row count after a full drain.
- `WithDrainOnEarlyStop`: optionally drains remaining rows after an early consumer stop so stats can be populated.
- `Rows`: adapt already-built rows for tests and virtual result sets.
- `SliceToRowSeq`: adapt existing `[]*spanner.Row` fixtures for downstream tests and fakes.

## RowIterator lifecycle

Cloud Spanner metadata is populated after the first `Next` call. Query plan,
query stats, and DML row count are populated only after `Next` returns
`iterator.Done`.

`RowIteratorSeq` keeps those rules explicit:

```go
var result spaniter.RowIteratorResult
rows := spaniter.RowIteratorSeq(rowIter, spaniter.WithResult(&result))

for row, err := range rows {
	if err != nil {
		return err
	}
	_ = row
}
_ = result.Metadata
_ = result.Stats
_ = result.RowsRead
```

Each yielded pair is either a non-nil row with a nil error, or a nil row with a
non-nil terminal error. After yielding a non-nil error, the sequence stops; code
should stop processing on the first error.

`RowIteratorSeq` can call `Stop` only after the sequence starts running. If the
returned lazy sequence is never invoked, callers must still retain responsibility
for stopping the original `RowIterator`.

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

```go
rows := spaniter.RowIteratorSeq(rowIter,
	spaniter.WithDrainOnEarlyStop(),
	spaniter.WithOnStats(func(got spaniter.Stats) {
		stats = got
	}),
)
```

## Example: spanvalue/writer

`spanvalue/writer` already accepts `iter.Seq2[*spanner.Row, error]` through
`RunRowSeqDeferredMetadata`. Capture metadata from `spaniter` and pass it as the
deferred metadata function:

```go
var spannerResult spaniter.RowIteratorResult
rows := spaniter.RowIteratorSeq(rowIter, spaniter.WithResult(&spannerResult))

result, err := writer.RunRowSeqDeferredMetadata(
	func() *sppb.ResultSetMetadata { return spannerResult.Metadata },
	rows,
	writer.RowIteratorHooksFromWriter(w),
)
_ = result
_ = err
_ = spannerResult.Stats
```

This keeps `spaniter` independent of formatting packages while letting
`spanvalue` reuse the standard iterator stream directly. Spanner query plan,
query stats, and DML row count come from `spaniter`'s `WithResult` or
`WithOnStats`; spanvalue's generic row-sequence result should not be expected to
populate Spanner-specific stats.

## Development

```bash
go test ./...
```
