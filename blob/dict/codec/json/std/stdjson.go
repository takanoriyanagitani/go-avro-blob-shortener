package stdjson

import (
	"bytes"
	"context"
	"encoding/json"

	. "github.com/takanoriyanagitani/go-avro-blob-shortener/util"

	bd "github.com/takanoriyanagitani/go-avro-blob-shortener/blob/dict"
)

func MapDecoderNew() bd.MapDecoder {
	buf := bd.OriginalMap{}
	return func(blob []byte) IO[bd.OriginalMap] {
		return func(_ context.Context) (bd.OriginalMap, error) {
			clear(buf)
			e := json.Unmarshal(blob, &buf)
			return buf, e
		}
	}
}

func MapEncoderNew() bd.MapEncoder {
	var buf bytes.Buffer
	var enc *json.Encoder = json.NewEncoder(&buf)

	return func(s bd.SmallerMap) IO[[]byte] {
		return func(_ context.Context) ([]byte, error) {
			buf.Reset()

			e := enc.Encode(map[string]any(s))
			return buf.Bytes(), e
		}
	}
}
