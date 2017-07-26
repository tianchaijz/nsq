package cdn

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/httpdown"
)

type StatsData struct {
	Aggs     []string                          `json:"aggs"`
	Hostname string                            `json:"hostname"`
	Data     map[string]map[string]interface{} `json:"data"`
}

// {hostname: {hash: {key: value}}}
type StatsDataAgg map[string]map[string]map[string]interface{}
type HTTPHanlder func(w http.ResponseWriter, r *http.Request)
type SendFunc func(data []byte, err error)

type StatsAgg struct {
	serverAddr    string
	serverRunning bool

	flushInterval time.Duration

	readTimeout time.Duration
	stopTimeout time.Duration
	killTimeout time.Duration

	dataChan         chan []byte
	stopChan         chan bool
	stopResponseChan chan bool
	stopServerChan   chan bool

	sendFunc SendFunc
}

func (sa *StatsAgg) Send(sda *StatsDataAgg) {
	if len(*sda) == 0 {
		return
	}

	for _, group := range *sda {
		for _, item := range group {
			data, err := json.Marshal(item)
			sa.sendFunc(data, err)
		}
	}
}

func doAgg(sda *StatsDataAgg, pack *[]byte) {
	if len(*pack) == 0 {
		return
	}

	var sd StatsData
	if err := json.Unmarshal(*pack, &sd); err != nil {
		fmt.Fprintf(os.Stderr, "[ERR] invalid json: %s - %v\n", string(*pack), err)
		return
	}

	if sd.Hostname == "" {
		return
	}

	for _, item := range sd.Data {
		item["hostname"] = sd.Hostname
	}

	if group, ok := (*sda)[sd.Hostname]; ok {
		for hash, item := range sd.Data {
			if itemAgg, ok := group[hash]; ok {
				for _, agg := range sd.Aggs {
					if m, ok := itemAgg[agg].(float64); ok {
						if n, ok := item[agg].(float64); ok {
							itemAgg[agg] = m + n
						}
					}
				}
			} else {
				group[hash] = item
			}
		}
	} else {
		(*sda)[sd.Hostname] = sd.Data
	}
}

func (sa *StatsAgg) aggregation() {
	ticker := time.NewTicker(sa.flushInterval)
	sda := make(StatsDataAgg)

	send := func() {
		sa.Send(&sda)
		sda = make(StatsDataAgg)
	}

	for {
		select {
		case <-ticker.C:
			send()
		case data := <-sa.dataChan:
			doAgg(&sda, &data)
		case <-sa.stopChan:
			send()
			sa.stopResponseChan <- true
			return
		}
	}
}

func (sa *StatsAgg) Run() {
	server := &http.Server{
		Addr:        sa.serverAddr,
		ReadTimeout: sa.readTimeout,
	}

	hd := &httpdown.HTTP{
		StopTimeout: sa.stopTimeout,
		KillTimeout: sa.killTimeout,
	}

	server.Handler = http.HandlerFunc(newHandlerFunc(sa.dataChan))
	go sa.aggregation()

	sa.serverRunning = true

	if err := httpdown.ListenAndServe(server, hd, sa.stopServerChan); err != nil {
		panic(err)
	}

	sa.serverRunning = false
}

func (sa *StatsAgg) Stop() {
	if !sa.serverRunning {
		return
	}

	sa.stopServerChan <- true

	sa.stopChan <- true
	<-sa.stopResponseChan
}

func newHandlerFunc(dataChan chan<- []byte) HTTPHanlder {
	return func(w http.ResponseWriter, r *http.Request) {
		if body, err := ioutil.ReadAll(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else {
			dataChan <- body
			w.WriteHeader(http.StatusOK)
		}
	}
}

func NewStatsAgg(serverAddr string, sendFunc SendFunc, flushInterval, readTimeout, stopTimeout, killTimeout time.Duration) *StatsAgg {
	sa := &StatsAgg{
		serverAddr:    serverAddr,
		serverRunning: false,

		flushInterval: flushInterval,

		readTimeout: readTimeout,
		killTimeout: killTimeout,
		stopTimeout: stopTimeout,

		sendFunc: sendFunc,
	}

	sa.dataChan = make(chan []byte, 256)
	sa.stopChan = make(chan bool)
	sa.stopResponseChan = make(chan bool)
	sa.stopServerChan = make(chan bool)

	return sa
}
