package rmkeys

import (
	"context"
	"iter"
	"maps"

	. "github.com/takanoriyanagitani/go-avro-blob-shortener/util"

	bd "github.com/takanoriyanagitani/go-avro-blob-shortener/blob/dict"
)

type RemoveKeySet map[string]struct{}

func (r RemoveKeySet) ToMapToSmaller() bd.MapToSmaller {
	buf := bd.SmallerMap{}
	return func(o bd.OriginalMap) IO[bd.SmallerMap] {
		return func(_ context.Context) (bd.SmallerMap, error) {
			clear(buf)

			for key, val := range o {
				_, found := r[key]
				if found {
					continue
				}

				buf[key] = val
			}
			return buf, nil
		}
	}
}

type RemoveKeys []string

func (r RemoveKeys) ToSet() map[string]struct{} {
	var i iter.Seq2[string, struct{}] = func(
		yield func(string, struct{}) bool,
	) {
		for _, key := range r {
			yield(key, struct{}{})
		}
	}
	return maps.Collect(i)
}

func (r RemoveKeys) ToMapToSmaller() bd.MapToSmaller {
	return RemoveKeySet(r.ToSet()).ToMapToSmaller()
}
