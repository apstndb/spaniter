package spaniter

// StatsEncoding selects how captured stats are converted to protobuf
// ResultSetStats when using [RowIteratorResult.StatsProto] or
// [RowIteratorResult.ResultSet].
//
// Set encoding with [WithStatsEncoding] when draining a RowIterator.
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

// WithStatsEncoding configures how [RowIteratorResult.StatsProto] encodes row
// counts for a drained iterator. The default is [StatsEncodingDefault].
func WithStatsEncoding(enc StatsEncoding) Option {
	return func(cfg *config) {
		cfg.statsEncoding = enc
	}
}

func (enc StatsEncoding) encodeRowCount(rowCount int64) bool {
	switch enc {
	case StatsEncodingDMLExact:
		return true
	default:
		return rowCount != 0
	}
}
