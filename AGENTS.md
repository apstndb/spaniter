# Agent instructions for `spaniter`

Go library: adapt Cloud Spanner `*spanner.RowIterator` streams to Go standard
iterators (`iter.Seq2[*spanner.Row, error]`). Target Go 1.23+ (`go.mod`).
Alias `sppb` = `cloud.google.com/go/spanner/apiv1/spannerpb`.

## Commands

Prefer `make check` before publishing or handing off changes. It verifies
formatting (`fmt-check`), then runs `go vet ./...`, `go build ./...`, and
`go test ./...`.

Useful narrower commands:

- `go test ./...`
- `go test . -run '^TestName$'`
- `make fmt`

## Package boundary

- Keep `spaniter` lower-level than `spanvalue`: no formatting policy, no output
  formats, no table/export orchestration, and no dependency on `spanvalue`.
- Use `spanvalue/writer` only as caller-side composition examples through
  `iter.Seq2`; do not make it a dependency.
- Keep references to sibling packages short and representative. The core docs
  should explain `spaniter` on its own terms.

## Iterator lifecycle

- `RowIteratorSeq` owns the `*spanner.RowIterator` only after the returned lazy
  sequence is invoked; if the sequence is never consumed, the caller is still
  responsible for `Stop`.
- Metadata appears after the first `Next` call, including empty result sets
  after `iterator.Done`; stats are available only after a successful full drain.
- `WithDrainOnEarlyStop` intentionally reads remaining rows after an early
  consumer stop. Keep docs explicit about the extra read work and the fact that
  post-stop drain errors cannot be yielded.
- `DrainRowIterator` is the path for callers that want metadata/stats without
  row values. It still consumes the stream because the Go client populates stats
  only after `iterator.Done`.

## Stats API

- `Stats` mirrors public `spanner.RowIterator` fields; values are not
  deep-copied and should be treated as read-only.
- `Stats.ResultSetStats` converts to protobuf `ResultSetStats` for downstream
  code that needs that shape. It returns `nil, nil` when there are no fields to
  encode. Preserve the distinction between absent `QueryStats` (`nil`) and
  present empty query stats (`map[string]any{}`).
- `Stats` cannot distinguish absent row count from `row_count_exact:0` because
  the Go client exposes row count as a plain `int64`.
- `Stats.ResultSetStatsForDML` is for standard DML callers that need
  `row_count_exact`, including zero. Do not imply partitioned DML support:
  `Client.PartitionedUpdate` returns counts directly rather than through
  `RowIterator`.
- `Stats.HasResultSetStats` is deprecated; call `ResultSetStats` and check for
  a nil message instead.
- Avoid adding a public `IsZero() bool` method unless you intentionally want
  JSON/YAML zero-value hooks to use it.

## Tests

- Use `t.Parallel()` for independent tests.
- Keep nil-vs-empty query stats and exact-zero DML row count covered when
  changing stats conversion behavior.
- Prefer Spanner-like query stats fixtures: values are wire strings in real
  query stats maps.

## Releases

- GitHub Releases are the source of release notes.
- Keep public release notes user-facing; do not mention internal reviewer or
  Oracle provenance.
- Verify release tags with `GOPROXY=direct go list -m
  github.com/apstndb/spaniter@<version>`.
