package avro_test

import "time"

type TestInterface interface {
	SomeFunc() int
}

type TestRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

type PacketInfoRecord struct {
	payload   []byte     `avro:"payload"`
	length    int        `avro:"length"`
	timestamp *time.Time `avro:"timestamp"`
	comment   string     `avro:"comment"`
	action    string     `avro:"action"`
}

func (*TestRecord) SomeFunc() int {
	return 0
}

type TestPartialRecord struct {
	B string `avro:"b"`
}

type TestNestedRecord struct {
	A TestRecord `avro:"a"`
	B TestRecord `avro:"b"`
}

type TestUnion struct {
	A interface{} `avro:"a"`
}

type TestEmbeddedRecord struct {
	TestEmbed

	B string `avro:"b"`
}

type TestEmbeddedPtrRecord struct {
	*TestEmbed

	B string `avro:"b"`
}

type TestEmbed struct {
	A int64 `avro:"a"`
}

type TestEmbedInt int

type TestEmbeddedIntRecord struct {
	TestEmbedInt

	B string `avro:"b"`
}

type TestUnexportedRecord struct {
	A int64  `avro:"a"`
	b string `avro:"b"`
}
