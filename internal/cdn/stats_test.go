package cdn

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"testing"
	"time"
)

const (
	ServerAddr = "127.0.0.1:5152"

	FlushInterval = 2 * time.Second
	SleepInterval = 5 * time.Second
	ReadTimeout   = 2 * time.Second
	StopTimeout   = 2 * time.Second
	KillTimeout   = 2 * time.Second

	PostCount = 100
	PostDataA = `{"data":{"0":{"0":1,"timestamp":1501230770,"client_province":"本机地址","domain":"example.com","bytes_sent":365,"server_protocol":"HTTP/1.1","count":1,"client_isp":"","method":"GET","client_country":"本机地址","service_name":"example","client_city":"","status":400,"cache_status":"MISS","content_type":"html","scheme":"https","body_bytes_sent":179,"request_time":0.001},"1669892258":{"timestamp":1501230760,"client_province":"本机地址","domain":"example.com","bytes_sent":16680,"server_protocol":"HTTP/1.1","count":20,"client_isp":"","method":"GET","client_country":"本机地址","service_name":"example","client_city":"","status":200,"cache_status":"MISS","content_type":"octet-stream","scheme":"http","body_bytes_sent":380,"request_time":0.012},"1892599272":{"timestamp":1501230770,"client_province":"本机地址","domain":"example.com","bytes_sent":663,"server_protocol":"HTTP/1.1","count":1,"client_isp":"","method":"GET","client_country":"本机地址","service_name":"example","client_city":"","status":200,"cache_status":"MISS","content_type":"octet-stream","scheme":"https","body_bytes_sent":19,"request_time":0},"-1756217838":{"timestamp":1501230760,"service_name":"example","domain":"example.com","bytes_sent":361,"server_protocol":"HTTP/1.1","count":1,"client_isp":"-","method":"GET","cache_status":"MISS","client_country":"-","client_city":"-","status":403,"client_province":"-","content_type":"html","scheme":"http","body_bytes_sent":175,"request_time":0.009},"-2075263067":{"timestamp":1501230760,"client_province":"本机地址","domain":"example.com","bytes_sent":3276,"server_protocol":"HTTP/1.1","count":9,"client_isp":"","method":"GET","client_country":"本机地址","service_name":"example","client_city":"","status":403,"cache_status":"MISS","content_type":"html","scheme":"http","body_bytes_sent":1575,"request_time":0.003},"-1332098106":{"timestamp":1501230770,"client_province":"本机地址","domain":"example.com","bytes_sent":17266,"server_protocol":"HTTP/1.1","count":23,"client_isp":"","method":"GET","client_country":"本机地址","service_name":"example","client_city":"","status":200,"cache_status":"MISS","content_type":"octet-stream","scheme":"http","body_bytes_sent":737,"request_time":0.016}},"count":6,"aggs":["bytes_sent","request_time","body_bytes_sent","count","NOT_EXISTS"],"hostname":"local"}`
	// 2
	PostDataB = `{"data":{"117218950":{"timestamp":1501230930,"client_province":"本机地址","domain":"example.com","bytes_sent":365,"server_protocol":"HTTP/1.1","count":1,"client_isp":"","method":"GET","client_country":"本机地址","service_name":"example","client_city":"","status":400,"cache_status":"MISS","content_type":"html","scheme":"http","body_bytes_sent":179,"request_time":0},"536993123":{"timestamp":1501230920,"client_province":"本机地址","domain":"example.com","bytes_sent":186,"server_protocol":"HTTP/1.1","count":1,"client_isp":"","method":"HEAD","client_country":"本机地址","service_name":"example","client_city":"","status":400,"cache_status":"MISS","content_type":"html","scheme":"http","body_bytes_sent":0,"request_time":0.01}},"count":2,"aggs":["bytes_sent","request_time","body_bytes_sent","count"],"hostname":"local"}`
	// 1
	PostDataC = `{"data":{"-1962166580":{"timestamp":1501230930,"client_province":"本机地址","domain":"example.com","bytes_sent":365,"server_protocol":"HTTP/1.1","count":1,"client_isp":"","method":"POST","client_country":"本机地址","service_name":"example","client_city":"","status":400,"cache_status":"MISS","content_type":"html","scheme":"http","body_bytes_sent":179,"request_time":0}},"count":1,"aggs":["bytes_sent","request_time","body_bytes_sent","count"],"hostname":"local"}`
)

type ItemJson map[string]interface{}
type StatsJson struct {
	Count     int                 `json:"count"`
	Timestamp int                 `json:"timestamp"`
	Hostname  string              `json:"hostname"`
	Data      map[string]ItemJson `json:"data"`
	Aggs      []string            `json:"aggs"`
}

func postData(data string) {
	url := fmt.Sprintf("http://%s/", ServerAddr)
	req, _ := http.NewRequest("POST", url, bytes.NewBufferString(data))
	client := &http.Client{}
	resp, _ := client.Do(req)

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != 200 {
		panic(fmt.Sprintf("unexpected status: %d", resp.StatusCode))
	}
}

func newSendFunc(recvChan chan<- []byte) SendFunc {
	return func(data []byte, err error) {
		if err != nil {
			panic(err.Error())
		}

		recvChan <- data
	}
}

func TestStatsAgg(t *testing.T) {
	recvChan := make(chan []byte, 256)
	sendFunc := newSendFunc(recvChan)

	sda := NewStatsAgg(ServerAddr, sendFunc, FlushInterval, ReadTimeout, StopTimeout, KillTimeout)
	go sda.Run()

	var statsJson StatsJson
	json.Unmarshal([]byte(PostDataA), &statsJson)

	itemJson := statsJson.Data["0"]

	for i := 0; i < PostCount; i++ {
		postData(PostDataA)
	}

	postData(PostDataB)
	postData(PostDataC)

	go func() {
		time.Sleep(SleepInterval)
		close(recvChan)
	}()

	count := 0
	var itemJsonAgg ItemJson
	for data := range recvChan {
		var ij ItemJson
		json.Unmarshal(data, &ij)
		if _, ok := ij["0"]; ok {
			itemJsonAgg = ij
		}
		count++
	}

	fmt.Printf("post count: %d\n", PostCount)
	fmt.Printf("agg count: %d\n", count)

	if count != statsJson.Count+3 {
		t.Errorf("agg count failed: %d != %d", count, statsJson.Count+3)
	}

	checkAgg := func(key string) {
		v := itemJson[key].(float64)
		va := itemJsonAgg[key].(float64)
		fmt.Printf("check [%s] value=%f agg=%f value*count=%f\n", key, v, va, v*PostCount)
		if math.Abs(va-v*PostCount) > 1e-9 {
			t.Errorf("agg [%s] failed: %f != %f", key, va, v*PostCount)
		}
	}

	for _, key := range statsJson.Aggs {
		if key != "NOT_EXISTS" {
			checkAgg(key)
		}
	}

	hostname := itemJsonAgg["hostname"]
	if hostname != statsJson.Hostname {
		t.Errorf("hostname: %s != %s", hostname, statsJson.Hostname)
	}

	sda.Stop()
}
