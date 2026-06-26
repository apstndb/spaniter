package spaniter

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// ResultSet builds a protobuf ResultSet from materialized rows and iterator
// lifecycle data captured while draining a RowIterator.
//
// rows may be nil when row values are intentionally omitted. statsEnc selects
// row-count encoding; use [StatsEncodingDMLExact] only for executed standard DML.
func (r RowIteratorResult) ResultSet(rows []*structpb.ListValue, statsEnc StatsEncoding) (*sppb.ResultSet, error) {
	out := &sppb.ResultSet{
		Rows:     rows,
		Metadata: r.Metadata,
	}
	resultStats, err := r.Stats.ResultSetStatsEncoded(statsEnc)
	if err != nil {
		return nil, err
	}
	out.Stats = resultStats
	return out, nil
}
