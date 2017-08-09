// This is an NSQ client that reads the specified topic/channel
// and performs HTTP requests (GET/POST) to the specified endpoints

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/timer_metrics"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
)

const (
	ModeAll = iota
	ModeRoundRobin
	ModeHostPool
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic       = flag.String("topic", "", "nsq topic")
	channel     = flag.String("channel", "nsq_to_elasticsearch", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	numPublishers      = flag.Int("n", 100, "number of concurrent publishers")
	mode               = flag.String("mode", "hostpool", "the upstream request mode options: round-robin, hostpool (default), epsilon-greedy")
	sample             = flag.Float64("sample", 1.0, "% of messages to publish (float b/w 0 -> 1)")
	httpConnectTimeout = flag.Duration("http-client-connect-timeout", 2*time.Second, "timeout for HTTP connect")
	httpRequestTimeout = flag.Duration("http-client-request-timeout", 20*time.Second, "timeout for HTTP request")
	flushInterval      = flag.Duration("flush-interval", 2*time.Second, "flush interval")
	statusEvery        = flag.Int("status-every", 250, "the # of requests between logging status (per handler), 0 disables")
	contentType        = flag.String("content-type", "application/octet-stream", "the Content-Type used for POST requests")
	authorization      = flag.String("authorization", "", "the basic authorization used for POST requests")

	postAddrs        = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&postAddrs, "post", "ElasticSearch bulk HTTP API to make a POST request to. data will be in the body (may be given multiple times)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")

	flag.StringVar(&esIndexName, "es-index-name", "nsq", "elasticsearch: the index name of doc")
	flag.StringVar(&esDocType, "es-doc-type", "nsq", "elasticsearch: the type name of doc")
}

type Publisher interface {
	Publish(string, []byte) error
}

type PublishHandler struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	counter uint64

	Publisher
	addresses app.StringArray
	mode      int
	hostPool  hostpool.HostPool

	perAddressStatus map[string]*timer_metrics.TimerMetrics
	timermetrics     *timer_metrics.TimerMetrics

	buf              *bytes.Buffer
	flushInterval    time.Duration
	recvChan         chan []byte
	stopChan         chan bool
	stopResponseChan chan bool
}

func (ph *PublishHandler) committer() {
	ticker := time.NewTicker(ph.flushInterval)
	for {
		select {
		case <-ticker.C:
			ph.send(false)
		case doc := <-ph.recvChan:
			BulkPopulate(ph.buf, &doc)
		case <-ph.stopChan:
			ph.send(true)
			ph.stopResponseChan <- true
			return
		}
	}
}

func (ph *PublishHandler) send(stopping bool) {
	if ph.buf.Len() == 0 {
		return
	}

	var err error
	for {
		if err = ph.doSend(); err == nil {
			ph.buf.Reset()
			return
		}

		log.Printf("send error: %v\n", err)

		if stopping {
			return
		}

		time.Sleep(5 * time.Second)
	}
}

func (ph *PublishHandler) doSend() error {
	startTime := time.Now()
	switch ph.mode {
	case ModeAll:
		for _, addr := range ph.addresses {
			st := time.Now()
			err := ph.Publish(addr, ph.buf.Bytes())
			if err != nil {
				return err
			}
			ph.perAddressStatus[addr].Status(st)
		}
	case ModeRoundRobin:
		counter := atomic.AddUint64(&ph.counter, 1)
		idx := counter % uint64(len(ph.addresses))
		addr := ph.addresses[idx]
		err := ph.Publish(addr, ph.buf.Bytes())
		if err != nil {
			return err
		}
		ph.perAddressStatus[addr].Status(startTime)
	case ModeHostPool:
		hostPoolResponse := ph.hostPool.Get()
		addr := hostPoolResponse.Host()
		err := ph.Publish(addr, ph.buf.Bytes())
		hostPoolResponse.Mark(err)
		if err != nil {
			return err
		}
		ph.perAddressStatus[addr].Status(startTime)
	}
	ph.timermetrics.Status(startTime)
	return nil
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	if *sample < 1.0 && rand.Float64() > *sample {
		return nil
	}

	ph.recvChan <- m.Body

	return nil
}

type PostPublisher struct{}

func (p *PostPublisher) Publish(addr string, msg []byte) error {
	buf := bytes.NewBuffer(msg)
	resp, err := HTTPPost(addr, buf)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body []byte
		if body, err = ioutil.ReadAll(resp.Body); err == nil {
			return fmt.Errorf("%s", string(body))
		}
		return fmt.Errorf("got status code %d", resp.StatusCode)
	}

	io.Copy(ioutil.Discard, resp.Body)
	return nil
}

func main() {
	var publisher Publisher
	var addresses app.StringArray
	var selectedMode int

	cfg := nsq.NewConfig()

	flag.Var(&nsq.ConfigFlag{cfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_elasticsearch v%s\n", version.Binary)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatal("--topic and --channel are required")
	}

	if *contentType != flag.Lookup("content-type").DefValue {
		if len(postAddrs) == 0 {
			log.Fatal("--content-type only used with --post")
		}
		if len(*contentType) == 0 {
			log.Fatal("--content-type requires a value when used")
		}
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if len(postAddrs) == 0 {
		log.Fatal("--post required")
	}

	publisher = &PostPublisher{}
	addresses = postAddrs

	switch *mode {
	case "round-robin":
		selectedMode = ModeRoundRobin
	case "hostpool", "epsilon-greedy":
		selectedMode = ModeHostPool
	}

	if *sample > 1.0 || *sample < 0.0 {
		log.Fatal("ERROR: --sample must be between 0.0 and 1.0")
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	cfg.UserAgent = fmt.Sprintf("nsq_to_elasticsearch/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(err)
	}

	perAddressStatus := make(map[string]*timer_metrics.TimerMetrics)
	if len(addresses) == 1 {
		// disable since there is only one address
		perAddressStatus[addresses[0]] = timer_metrics.NewTimerMetrics(0, "")
	} else {
		for _, a := range addresses {
			perAddressStatus[a] = timer_metrics.NewTimerMetrics(*statusEvery,
				fmt.Sprintf("[%s]:", a))
		}
	}

	hostPool := hostpool.New(addresses)
	if *mode == "epsilon-greedy" {
		hostPool = hostpool.NewEpsilonGreedy(addresses, 0, &hostpool.LinearEpsilonValueCalculator{})
	}

	handler := &PublishHandler{
		Publisher:        publisher,
		addresses:        addresses,
		mode:             selectedMode,
		hostPool:         hostPool,
		perAddressStatus: perAddressStatus,
		timermetrics:     timer_metrics.NewTimerMetrics(*statusEvery, "[aggregate]:"),

		buf:              &bytes.Buffer{},
		flushInterval:    *flushInterval,
		recvChan:         make(chan []byte, 256),
		stopChan:         make(chan bool),
		stopResponseChan: make(chan bool),
	}

	consumer.AddConcurrentHandlers(handler, *numPublishers)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	go handler.committer()

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-termChan:
			consumer.Stop()
		}
	}

	handler.stopChan <- true
	<-handler.stopResponseChan
}
