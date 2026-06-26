package spaniter

import sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

// StatsEncoding selects how [Stats] is converted to protobuf ResultSetStats.
type StatsEncoding int

const (
	// StatsEncodingDefault uses default query semantics: omit row_count_exact when
	// RowCount is zero because absent row count and exact zero are indistinguishable
	// on [cloud.google.com/go/spanner.RowIterator].
	StatsEncodingDefault StatsEncoding = iota
	// StatsEncodingDMLExact always encodes RowCount as row_count_exact, including
	// zero. Use only when the caller knows Stats came from executed standard DML
	// (not PLAN and not read-only queries).
	StatsEncodingDMLExact
)

// ResultSetStatsEncoded returns s as *sppb.ResultSetStats using enc.
//
// Most callers should use [Stats.ResultSetStats] or pass [StatsEncodingDefault].
// Use [StatsEncodingDMLExact] only for executed standard DML where
// row_count_exact:0 must be preserved.
func (s Stats) ResultSetStatsEncoded(enc StatsEncoding) (*sppb.ResultSetStats, error) {
	switch enc {
	case StatsEncodingDMLExact:
		return s.resultSetStats(true)
	default:
		return s.resultSetStats(s.RowCount != 0)
	}
}
