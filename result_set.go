package spaniter

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// StatsProto returns captured stats as *sppb.ResultSetStats using the encoding
// configured by [WithStatsEncoding] when the iterator was drained.
//
// Row counts are encoded only after stats were captured at [iterator.Done].
// Query plan and query stats encode from the captured [Stats] value whenever
// present. Partial results from errors omit row counts even when
// [StatsEncodingDMLExact] is configured.
func (r RowIteratorResult) StatsProto() (*sppb.ResultSetStats, error) {
	encodeRowCount := false
	if r.statsCaptured {
		encodeRowCount = r.statsEnc.encodeRowCount(r.Stats.RowCount)
	}
	return r.Stats.resultSetStats(encodeRowCount)
}

// ResultSet builds a protobuf ResultSet from materialized rows and iterator
// lifecycle data captured while draining a RowIterator.
//
// rows may be nil when row values are intentionally omitted. Stats encoding
// comes from [WithStatsEncoding] on the drain options.
func (r RowIteratorResult) ResultSet(rows []*structpb.ListValue) (*sppb.ResultSet, error) {
	out := &sppb.ResultSet{
		Rows:     rows,
		Metadata: r.Metadata,
	}
	resultStats, err := r.StatsProto()
	if err != nil {
		return nil, err
	}
	out.Stats = resultStats
	return out, nil
}
