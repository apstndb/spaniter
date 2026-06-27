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
// The returned stop function releases the RowIterator. If stop runs before the
// first pull, stop calls RowIterator.Stop directly because [iter.Pull2] has not
// started the sequence yet. After the first pull, stop signals the sequence and
// RowIteratorSeq owns Stop when the sequence goroutine exits.
func PullRowIteratorSeq(rowIter *spanner.RowIterator, opts ...Option) (pull func() (*spanner.Row, error, bool), stop func()) {
	if rowIter == nil {
		return pullRowSourceSeq(nil, opts...)
	}
	return pullRowSourceSeq(spannerRowSource{rowIter}, opts...)
}

func pullRowSourceSeq(src rowSource, opts ...Option) (pull func() (*spanner.Row, error, bool), stop func()) {
	cfg := newConfig(opts)
	seq := rowSourceSeq(src, opts...)
	rawPull, rawStop := iter.Pull2(seq)

	var pulled bool
	var stopped bool
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		if !pulled {
			resetResult(cfg)
			if src != nil {
				src.stop()
			}
		}
		rawStop()
	}
	pull = func() (*spanner.Row, error, bool) {
		pulled = true
		row, err, ok := rawPull()
		if err != nil {
			stop()
			return nil, err, false
		}
		return row, nil, ok
	}
	return pull, stop
}
