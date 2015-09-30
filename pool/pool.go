package pool

import (
	"errors"
	"fmt"
	"time"

	"github.com/lysu/thriftpool-go/circuit"
	"github.com/lysu/thriftpool-go/log"
	"golang.org/x/net/context"
)

// Pool is a pool for resource
type Pool interface {
	// Get is use to get resource
	Get(ctx context.Context) (Resource, error)

	// Return is use to return resource to pool
	Return(ctx context.Context, resource Resource, forceClose bool) error

	// StatsJSON is used to take group pool stats info by json array
	StatsJSON() string
}

// ResourcePool is pool of resources
type ResourcePool struct {
	address        string
	factory        GroupFactory
	resources      chan resourceWrapper
	idleTimeout    time.Duration
	circuitBreaker *circuit.CircuitBreaker
}

// StatsJSON return stats info for resource pool as json
func (r *ResourcePool) StatsJSON() string {
	return fmt.Sprintf(`{"address": "%s", "resourceNum": %d}`, r.address, len(r.resources))
}

func (r *ResourcePool) available() bool {
	return len(r.resources) > 0
}

// NewResourcePool use to create pool for resource
func NewResourcePool(address string, factory GroupFactory, initNum, maxCap int, idleTimeout time.Duration, circuitBreaker *circuit.CircuitBreaker) *ResourcePool {
	if maxCap <= 0 {
		panic(errors.New("invalid of max capacity"))
	}
	var (
		resourcePool *ResourcePool
		err          error
	)
	resourcePool = &ResourcePool{
		factory:     factory,
		resources:   make(chan resourceWrapper, maxCap),
		idleTimeout: idleTimeout,
	}
	for i := 0; i < initNum; i++ {
		var res Resource
		res, err = factory()
		if err != nil {
			continue
		}
		log.Infof(nil, "Init resource %v", res)
		resourcePool.resources <- resourceWrapper{res, time.Now()}
	}
	resourcePool.address = address
	resourcePool.circuitBreaker = circuitBreaker
	return resourcePool
}

// Get resource from pool when meet max concurrent get will wait.
func (r *ResourcePool) Get(ctx context.Context) (Resource, error) {
	return r.get(ctx, true)
}

// Get resource from pool
func (r *ResourcePool) get(ctx context.Context, wait bool) (Resource, error) {

	select {
	case <-ctx.Done():
		return nil, ErrTimeout
	default:
	}

	var (
		wrapper resourceWrapper
		err     error
	)

	start := time.Now()
	for {
		select {
		case wrapper = <-r.resources:
			if time.Now().Sub(wrapper.timeUsed) >= r.idleTimeout {
				log.Warningf(ctx, "Get idle timeout resource, address: %s, av-size: %d,  waste-time: %d\n", wrapper.resource.Address(), len(r.resources), time.Now().Sub(start).Nanoseconds())
				r.closeConn(ctx, wrapper.resource)
				continue
			}
			log.Infof(ctx, "Get resouce from pool success, address: %v, av-size: %d,  waste-time: %d\n", wrapper.resource.Address(), len(r.resources), time.Now().Sub(start).Nanoseconds())
			return wrapper.resource, nil
		default:
			log.Warningf(ctx, "Have not enough resource in pool, start create resource, address: %s, av-size: %d\n", r.address, len(r.resources))
			var res Resource
			if res, err = r.factory(); err != nil {
				log.Errorf(ctx, "create resource failure, error: %v", err)
				return nil, err
			}
			log.Infof(ctx, "Create new resource successed, address: %s", res.Address())

			r.enqueue(ctx, res)

			log.Infof(ctx, "New resource back to queue sucessed, address: %s, av-size: %d", res.Address(), len(r.resources))

			continue
		}
	}
}

// Put is used to put resource to resource pool
// forceClose is use to force close and not return to pool
// but everytime call this method will give priv to other consumer to use resource
func (r *ResourcePool) Put(ctx context.Context, res Resource, forceClose bool) {

	if forceClose {
		r.closeConn(ctx, res)
		return
	}

	r.enqueue(ctx, res)

}

// enqueue is used to do real enqueue op
func (r *ResourcePool) enqueue(ctx context.Context, res Resource) {

	if r.address != res.Address() {
		log.Errorf(ctx, "FATAL: Conn %s back to wrong pool %s\n", r.address, res.Address())
	}

	start := time.Now()
	log.Infof(ctx, "Start enqueue, address: %s, av-size: %d\n", res.Address(), len(r.resources))
	select {
	case r.resources <- resourceWrapper{res, time.Now()}:
		log.Infof(ctx, "Put resource back, address: %s, av-size: %d,  waste-time: %d\n", res.Address(), len(r.resources), time.Now().Sub(start).Nanoseconds())
		return
	default:
		log.Warningf(ctx, "Put resource back failure, because full close it, address: %s, waste-time: %d\n", res.Address(), len(r.resources))
		r.closeConn(ctx, res)
	}

}

func (r *ResourcePool) closeConn(ctx context.Context, resource Resource) {

	if resource == nil {
		return
	}

	err := resource.RawClose()
	if err != nil {
		log.Errorf(ctx, "Close connection but failured, address: %s", r.address)
	}
	log.Infof(ctx, "Close resource: %s", resource.Address())

}
