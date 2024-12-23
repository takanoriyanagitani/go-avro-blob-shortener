package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-blob-shortener/util"

	sb "github.com/takanoriyanagitani/go-avro-blob-shortener/blob"

	dh "github.com/takanoriyanagitani/go-avro-blob-shortener/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-blob-shortener/avro/enc/hamba"

	bd "github.com/takanoriyanagitani/go-avro-blob-shortener/blob/dict"
	js "github.com/takanoriyanagitani/go-avro-blob-shortener/blob/dict/codec/json/std"
	dr "github.com/takanoriyanagitani/go-avro-blob-shortener/blob/dict/remove"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return func(filename string) IO[string] {
		return func(_ context.Context) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()

			limited := &io.LimitedReader{
				R: f,
				N: limit,
			}

			var buf strings.Builder

			_, e = io.Copy(&buf, limited)
			return buf.String(), e
		}
	}
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var removeKeys IO[[]string] = Of(os.Args[1:])

var map2smaller IO[bd.MapToSmaller] = Bind(
	removeKeys,
	Lift(func(keys []string) (bd.MapToSmaller, error) {
		return dr.RemoveKeys(keys).ToMapToSmaller(), nil
	}),
)

var blob2map bd.MapDecoder = js.MapDecoderNew()
var map2blob bd.MapEncoder = js.MapEncoderNew()

var mapSerde bd.MapSerde = bd.MapSerde{
	MapDecoder: blob2map,
	MapEncoder: map2blob,
}

var blob2smaller IO[sb.BlobToSmaller] = Bind(
	map2smaller,
	Lift(func(m2s bd.MapToSmaller) (sb.BlobToSmaller, error) {
		return mapSerde.ToBlobToSmaller(m2s), nil
	}),
)

var stdin2avro2maps IO[iter.Seq2[map[string]any, error]] = dh.StdinToMapsDefault

type Config struct {
	BlobKey      string
	BlobToString bool
}

var config IO[Config] = Bind(
	All(
		EnvValByKey("ENV_BLOB_KEY"),
		EnvValByKey("ENV_CONVERT_BLOB_TO_STRING").Or(Of("false")),
	),
	Lift(func(s []string) (Config, error) {
		parsed, e := strconv.ParseBool(s[1])
		return Config{
			BlobKey:      s[0],
			BlobToString: parsed,
		}, e
	}),
)

var mapsWithSmallerBlob IO[iter.Seq2[map[string]any, error]] = Bind(
	stdin2avro2maps,
	func(
		original iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			blob2smaller,
			func(b2s sb.BlobToSmaller) IO[iter.Seq2[map[string]any, error]] {
				return Bind(
					config,
					func(cfg Config) IO[iter.Seq2[map[string]any, error]] {
						return b2s.MapsWithSmallerBlobEx(
							original,
							cfg.BlobKey,
							cfg.BlobToString,
						)
					},
				)
			},
		)
	},
)

var stdin2avro2maps2smaller2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			mapsWithSmallerBlob,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2avro2maps2smaller2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
