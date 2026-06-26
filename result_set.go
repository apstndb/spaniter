package spaniter

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// StatsProto returns captured stats as *sppb.ResultSetStats using the encoding
// configured by [WithStatsEncoding] when the iterator was drained.
func (r RowIteratorResult) StatsProto() (*sppb.ResultSetStats, error) {
	return r.Stats.resultSetStats(r.statsEnc.encodeRowCount(r.Stats.RowCount))
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
