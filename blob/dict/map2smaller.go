package map2smaller

import (
	"context"

	. "github.com/takanoriyanagitani/go-avro-blob-shortener/util"

	sb "github.com/takanoriyanagitani/go-avro-blob-shortener/blob"
)

type OriginalMap map[string]any
type SmallerMap map[string]any

type MapToSmaller func(OriginalMap) IO[SmallerMap]

type MapDecoder func([]byte) IO[OriginalMap]
type MapEncoder func(SmallerMap) IO[[]byte]

type MapSerde struct {
	MapDecoder
	MapEncoder
}

func (m MapSerde) ToBlobToSmaller(m2s MapToSmaller) sb.BlobToSmaller {
	return func(o sb.OriginalBlob) IO[sb.SmallerBlob] {
		return func(ctx context.Context) (sb.SmallerBlob, error) {
			original, e := m.MapDecoder(o)(ctx)
			if nil != e {
				return nil, e
			}

			smallerMap, e := m2s(original)(ctx)
			if nil != e {
				return nil, e
			}

			return m.MapEncoder(smallerMap)(ctx)
		}
	}
}
