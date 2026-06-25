package spaniter

import (
	"iter"

	"cloud.google.com/go/spanner"
)

// Rows adapts already-built rows to the fallible sequence shape used by
// [RowIteratorSeq]. Non-nil rows are yielded with a nil error. A nil row aborts
// the sequence by yielding [ErrNilRow].
//
// Row sources that can fail per row should produce their own [iter.Seq2]
// instead of pre-building a slice for Rows.
func Rows(rows ...*spanner.Row) iter.Seq2[*spanner.Row, error] {
	return func(yield func(*spanner.Row, error) bool) {
		for _, row := range rows {
			if row == nil {
				yield(nil, ErrNilRow)
				return
			}
			if !yield(row, nil) {
				return
			}
		}
	}
}

// SliceToRowSeq adapts an existing row slice to the fallible sequence shape
// used by [RowIteratorSeq].
//
// It exists for downstream tests, fakes, and virtual result sets that naturally
// store fixtures as []*spanner.Row. It is equivalent to Rows(rows...), including
// nil-row handling: nil rows yield [ErrNilRow] and abort the sequence.
func SliceToRowSeq(rows []*spanner.Row) iter.Seq2[*spanner.Row, error] {
	return Rows(rows...)
}
