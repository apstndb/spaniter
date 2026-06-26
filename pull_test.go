package spaniter

import (
	"errors"
	"iter"
	"testing"

	"cloud.google.com/go/spanner"
)

func TestPullRowIteratorSeqNilIterator(t *testing.T) {
	t.Parallel()

	pull, stop := PullRowIteratorSeq(nil)
	defer stop()

	_, err, ok := pull()
	if ok {
		t.Fatal("ok = true, want false")
	}
	if !errors.Is(err, ErrNilRowIterator) {
		t.Fatalf("err = %v, want %v", err, ErrNilRowIterator)
	}
}

func TestPullRowIteratorSeqPullNormalizesError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("terminal")
	src := &stubRowSource{
		rows:  []*spanner.Row{{}},
		md:    metadataWithColumnNames("id"),
		errAt: 1,
		err:   sentinel,
	}
	pull, _ := iterPull2Normalized(rowSourceSeq(src))

	if _, err, ok := pull(); !ok || err != nil {
		t.Fatalf("first pull = ok=%v err=%v", ok, err)
	}
	row, err, ok := pull()
	if row != nil {
		t.Fatalf("row = %v, want nil", row)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if ok {
		t.Fatal("ok = true, want false on terminal error")
	}
}

// iterPull2Normalized mirrors PullRowIteratorSeq's pull normalization for tests
// that already hold a sequence.
func iterPull2Normalized(seq iter.Seq2[*spanner.Row, error]) (func() (*spanner.Row, error, bool), func()) {
	rawPull, stop := iter.Pull2(seq)
	pull := func() (*spanner.Row, error, bool) {
		row, err, ok := rawPull()
		if err != nil {
			stop()
			return nil, err, false
		}
		return row, nil, ok
	}
	return pull, stop
}

func TestPullRowSourceSeqStopsBeforeFirstPull(t *testing.T) {
	t.Parallel()

	src := &stubRowSource{
		rows: []*spanner.Row{{}},
		md:   metadataWithColumnNames("id"),
	}
	_, stop := pullRowSourceSeq(src)
	stop()
	if !src.stopped {
		t.Fatal("source was not stopped before first pull")
	}
}

func TestPullRowSourceSeqStopsBeforeFirstPullResetsWithResult(t *testing.T) {
	t.Parallel()

	src := &stubRowSource{
		rows: []*spanner.Row{{}},
		md:   metadataWithColumnNames("id"),
	}
	var result RowIteratorResult
	result.Metadata = metadataWithColumnNames("stale")
	result.RowsRead = 99
	_, stop := pullRowSourceSeq(src, WithResult(&result))
	stop()
	if result.Metadata != nil || result.RowsRead != 0 {
		t.Fatalf("WithResult = %+v, want reset before first pull", result)
	}
}

func TestPullRowIteratorSeqStopsOnTerminalError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("terminal")
	src := &stubRowSource{
		rows:  []*spanner.Row{{}},
		md:    metadataWithColumnNames("id"),
		errAt: 1,
		err:   sentinel,
	}
	pull, _ := pullRowSourceSeq(src)

	if _, err, ok := pull(); !ok || err != nil {
		t.Fatalf("first pull = ok=%v err=%v", ok, err)
	}
	if _, err, ok := pull(); !errors.Is(err, sentinel) || ok {
		t.Fatalf("second pull = ok=%v err=%v", ok, err)
	}
	if !src.stopped {
		t.Fatal("source was not stopped after terminal error pull")
	}
}
