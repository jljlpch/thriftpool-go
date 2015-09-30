package test

import (
	"fmt"
	"myservice"
	"time"

	"github.com/lysu/thriftpool-go/circuit"
	"github.com/lysu/thriftpool-go/pool"
)

type ClientFactory func() (*myservice.MyServiceClient, error)

type thriftResource struct {
	addr           string
	client         *myservice.MyServiceClient
	pool           pool.Pool
	circuitBreaker *circuit.CircuitBreaker
}

func (r *thriftResource) Close() error {
	return r.pool.Return(nil, r, false)
}

func (r *thriftResource) RawClose() error {
	return r.client.Transport.Close()
}

func (r *thriftResource) Address() string {
	return r.addr
}

func (r *thriftResource) SetCircuitBreaker(circuitBreaker *circuit.CircuitBreaker) {
	r.circuitBreaker = circuitBreaker
}

func (r *thriftResource) CircuitBreaker() (*circuit.CircuitBreaker, error) {
	if r.circuitBreaker == nil {
		return nil, fmt.Errorf("CircuitBreaker init failure")
	}
	return r.circuitBreaker, nil
}

type Proxy struct {
	clientFactory ClientFactory
	pool          pool.Pool
}

type Opt struct {
	InitNum               int
	MaxCap                int
	MaxIdleTimeout        time.Duration
	ConTimeout            time.Duration
	SoTimeout             time.Duration
	BreakerTimeToOpenUnit time.Duration
	BreakerTimeToOpenMax  time.Duration
}

func New(address []string, clientFactory ClientFactory, opt Opt) *Proxy {
	p := &Proxy{
		clientFactory: clientFactory,
	}
	p.pool = pool.NewGroup(address, func(addr string, pool pool.Pool) (pool.Resource, error) {
		client, err := p.clientFactory()
		if err != nil {
			return nil, err
		}
		return &thriftResource{
			addr:   addr,
			pool:   pool,
			client: client,
		}, nil
	}, pool.Opt{
		InitNum:        opt.InitNum,
		MaxCap:         opt.MaxCap,
		MaxIdleTimeout: opt.MaxIdleTimeout,
		TimeToOpenUnit: opt.BreakerTimeToOpenUnit,
		TimeToOpenMax:  opt.BreakerTimeToOpenMax,
	})
	return p
}

func (p *Proxy) Ping() (err error) {
	poolResource, err := p.pool.Get(nil)
	if err != nil {
		return
	}
	defer p.pool.Return(nil, poolResource, false)
	return poolResource.(*thriftResource).client.Ping()
}

func (p *Proxy) GetResult_(id myservice.UUID, req *myservice.MyRequest) (r *myservice.MyResult_, err error) {
	poolResource, err := p.pool.Get(nil)
	if err != nil {
		return
	}
	defer p.pool.Return(nil, poolResource, false)
	return poolResource.(*thriftResource).client.GetResult_(id, req)
}
