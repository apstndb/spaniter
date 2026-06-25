package spaniter

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
)

func TestRows(t *testing.T) {
	t.Parallel()

	row1 := mustNewRow(t, []string{"id"}, []any{int64(1)})
	row2 := mustNewRow(t, []string{"id"}, []any{int64(2)})

	var got []int64
	for row, err := range Rows(row1, row2) {
		if err != nil {
			t.Fatal(err)
		}
		var id int64
		if err := row.Column(0, &id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0] != 1 || got[1] != 2 {
		t.Fatalf("got ids = [%d %d], want [1 2]", got[0], got[1])
	}
}

func TestRows_nilRowYieldsErrNilRow(t *testing.T) {
	t.Parallel()

	var gotErr error
	for _, err := range Rows(nil) {
		gotErr = err
	}
	if !errors.Is(gotErr, ErrNilRow) {
		t.Fatalf("error = %v, want ErrNilRow", gotErr)
	}
}

func TestSliceToRowSeqAdaptsFixtureSliceAndStopsEarly(t *testing.T) {
	t.Parallel()

	row1 := mustNewRow(t, []string{"id"}, []any{int64(1)})
	row2 := mustNewRow(t, []string{"id"}, []any{int64(2)})

	count := 0
	for _, err := range SliceToRowSeq([]*spanner.Row{row1, row2}) {
		if err != nil {
			t.Fatal(err)
		}
		count++
		break
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}
