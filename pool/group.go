package pool

import (
	"bytes"
	"fmt"
	"time"

	"github.com/lysu/thriftpool-go/circuit"
	"github.com/lysu/thriftpool-go/log"

	"golang.org/x/net/context"
)

// GroupFactory is an function to build resource for multi-address
type GroupFactory func() (Resource, error)

// Group manage a group of resource list
// every group contain many resources
type Group struct {
	poolIndex map[string]*ResourcePool
	breakers  *circuit.BreakerGroup
}

// Opt to build group
type Opt struct {
	InitNum        int
	MaxCap         int
	MaxIdleTimeout time.Duration
	TimeToOpenMax  time.Duration
	TimeToOpenUnit time.Duration
}

// NewGroup is used to construct pool group
func NewGroup(addressList []string, factory ResourceFactory, opt Opt) *Group {
	group := &Group{
		poolIndex: make(map[string]*ResourcePool),
	}
	group.breakers = circuit.NewBreakerGroup()
	for i := range addressList {
		address := addressList[i]
		breaker := circuit.NewCircuitBreaker(address, opt.TimeToOpenUnit, opt.TimeToOpenMax)
		pool := NewResourcePool(address, func() (Resource, error) {
			res, err := factory(address, group)
			if err != nil {
				return nil, err
			}
			res.SetCircuitBreaker(breaker)
			return res, nil
		}, opt.InitNum, opt.MaxCap, opt.MaxIdleTimeout, breaker)
		if !pool.available() {
			continue
		}
		group.poolIndex[address] = pool
		group.breakers.AddBreaker(breaker)
	}

	return group
}

// StatsJSON is used to take group pool stats info by json array
func (p *Group) StatsJSON() string {
	var buf bytes.Buffer
	buf.WriteString("[")
	for _, r := range p.poolIndex {
		buf.WriteString(r.StatsJSON())
		buf.WriteString(", ")
	}
	buf.WriteString("]")
	return buf.String()
}

// Get is used to get resource form group pool
func (p *Group) Get(ctx context.Context) (Resource, error) {
	pool, _, err := p.selectPool(ctx)
	if err == nil {
		return nil, err
	}
	res, err := pool.Get(ctx)
	if err != nil {
		log.Errorf(ctx, "Select pool failure, error: %v", err)
		return nil, err
	}
	log.Infof(ctx, "Selected pool %s with %d\n", res.Address())
	return res, nil
}

// Return is used to return resource to pool group
func (p *Group) Return(ctx context.Context, res Resource, forceClose bool) error {

	if res == nil {
		log.Errorf(ctx, "Return pool conn is null")
		return nil
	}

	pool, err := p.findPool(res.Address())
	if err != nil {
		log.Errorf(ctx, "Return pool conn can't found pool to them and close conn, address: %s", res.Address())
		res.Close()
	}

	pool.Put(ctx, res, forceClose)

	return nil

}

func (p *Group) selectPool(ctx context.Context) (*ResourcePool, *circuit.CircuitBreaker, error) {
	breaker, err := p.breakers.GetAvailable(ctx)
	return p.poolIndex[breaker.Addr], breaker, err
}

func (p *Group) findPool(address string) (*ResourcePool, error) {
	if pool, ok := p.poolIndex[address]; ok {
		return pool, nil
	}
	return nil, fmt.Errorf("Fail to get address %s", address)
}
