package main

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/version"
)

var httpclient *http.Client
var userAgent string

func init() {
	httpclient = &http.Client{Transport: http_api.NewDeadlineTransport(*httpConnectTimeout, *httpRequestTimeout), Timeout: *httpRequestTimeout}
	userAgent = fmt.Sprintf("nsq_to_elasticsearch v%s", version.Binary)
}

func HTTPPost(endpoint string, body *bytes.Buffer) (*http.Response, error) {
	req, err := http.NewRequest("POST", endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Add("Accept", "application/json")
	req.Header.Set("Content-Type", *contentType)

	if len(*authorization) > 0 {
		req.Header.Set("Authorization", "Basic "+*authorization)
	}

	return httpclient.Do(req)
}
