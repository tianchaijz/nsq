package main

import (
	"bytes"
	"fmt"
	"github.com/cactus/gostrftime"
	"time"
)

var (
	esIndexName string
	esDocType   string
)

func BulkPopulate(buf *bytes.Buffer, doc *[]byte) {
	now := time.Now()
	indexName := fmt.Sprintf("%s-%s", esIndexName, gostrftime.Format("%Y.%m.%d", now))

	buf.WriteString(`{"index":{"_index":"`)
	buf.WriteString(indexName)
	buf.WriteString(`","_type":"`)
	buf.WriteString(esDocType)
	buf.WriteString(`"}}`)
	buf.WriteByte('\n')
	buf.WriteString(string(*doc))
	buf.WriteByte('\n')
}
