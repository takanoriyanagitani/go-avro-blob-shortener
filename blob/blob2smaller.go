package blob2smaller

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-blob-shortener/util"
)

var (
	ErrInvalidBlob error = errors.New("invalid blob")
)

type OriginalBlob []byte
type SmallerBlob []byte

func (s SmallerBlob) Raw() []byte { return s }
func (s SmallerBlob) ToString(buf *strings.Builder) string {
	buf.Reset()
	_, _ = buf.Write(s.Raw()) // error is always nil or OOM
	return buf.String()
}

type SmallerToAny func(SmallerBlob) IO[any]

func SmallerToAnyDefault(sm SmallerBlob) IO[any] {
	return OfFn(func() any { return sm.Raw() })
}

func SmallerToAnyStringNew() SmallerToAny {
	var buf strings.Builder
	return func(sm SmallerBlob) IO[any] {
		return func(_ context.Context) (any, error) {
			return sm.ToString(&buf), nil
		}
	}
}

type BlobToSmaller func(OriginalBlob) IO[SmallerBlob]

func (b BlobToSmaller) AnyToSmaller(a any) IO[SmallerBlob] {
	var buf bytes.Buffer
	return func(ctx context.Context) (SmallerBlob, error) {
		buf.Reset()

		var original []byte
		switch t := a.(type) {
		case []byte:
			original = t
		case string:
			_, _ = buf.WriteString(t) // error is always nil or panic
			original = buf.Bytes()
		default:
			return nil, ErrInvalidBlob
		}

		return b(original)(ctx)
	}
}

func (b BlobToSmaller) MapsWithSmallerBlob(
	original iter.Seq2[map[string]any, error],
	blobKey string,
	smaller2any SmallerToAny,
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			buf := map[string]any{}
			for row, e := range original {
				clear(buf)

				if nil != e {
					yield(buf, e)
					return
				}

				for key, val := range row {
					var a any = val
					if blobKey == key {
						smaller, e := b.AnyToSmaller(a)(ctx)
						if nil != e {
							yield(buf, e)
							return
						}
						a, e = smaller2any(smaller)(ctx)
						if nil != e {
							yield(buf, e)
							return
						}
					}
					buf[key] = a
				}

				if !yield(buf, nil) {
					return
				}
			}
		}, nil
	}
}

func (b BlobToSmaller) MapsWithSmallerBlobDefault(
	original iter.Seq2[map[string]any, error],
	blobKey string,
) IO[iter.Seq2[map[string]any, error]] {
	return b.MapsWithSmallerBlob(
		original,
		blobKey,
		SmallerToAnyDefault,
	)
}

func (b BlobToSmaller) MapsWithSmallerBlobToString(
	original iter.Seq2[map[string]any, error],
	blobKey string,
) IO[iter.Seq2[map[string]any, error]] {
	return b.MapsWithSmallerBlob(
		original,
		blobKey,
		SmallerToAnyStringNew(),
	)
}

func (b BlobToSmaller) MapsWithSmallerBlobEx(
	original iter.Seq2[map[string]any, error],
	blobKey string,
	blobToString bool,
) IO[iter.Seq2[map[string]any, error]] {
	switch blobToString {
	case true:
		return b.MapsWithSmallerBlobToString(original, blobKey)
	default:
		return b.MapsWithSmallerBlobDefault(original, blobKey)
	}
}
