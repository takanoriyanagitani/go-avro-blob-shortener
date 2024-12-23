#!/bin/sh

export ENV_SCHEMA_FILENAME=sample.d/sample.avsc

jsons2avro(){
	cat sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/input.avro
}

#jsons2avro

export ENV_BLOB_KEY=data
export ENV_CONVERT_BLOB_TO_STRING=true
remove_keys="k3"
remove_keys="k1"
remove_keys="k2"

cat sample.d/input.avro |
	./avrojson2smaller ${remove_keys} |
	rq -aJ |
	jq \
		-c \
		--raw-output \
		.data |
	jq -c
