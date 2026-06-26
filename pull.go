package spaniter

import (
	"iter"

	"cloud.google.com/go/spanner"
)

// PullRowIteratorSeq adapts [RowIteratorSeq] for consumers using [iter.Pull2].
//
// The returned pull function normalizes terminal errors from the sequence: when
// RowIteratorSeq yields (nil, err), pull returns (nil, err, false) instead of
// (nil, err, true). This matches the usual "check err before ok" pattern and
// avoids treating iterator failures as EOF.
//
// The returned stop function signals the sequence to stop. RowIteratorSeq owns
// the RowIterator and calls Stop when the sequence goroutine exits; callers
// should not call [*spanner.RowIterator.Stop] directly after iteration starts.
func PullRowIteratorSeq(rowIter *spanner.RowIterator, opts ...Option) (pull func() (*spanner.Row, error, bool), stop func()) {
	seq := RowIteratorSeq(rowIter, opts...)
	rawPull, stop := iter.Pull2(seq)
	pull = func() (*spanner.Row, error, bool) {
		row, err, ok := rawPull()
		if err != nil {
			return nil, err, false
		}
		return row, nil, ok
	}
	return pull, stop
}
