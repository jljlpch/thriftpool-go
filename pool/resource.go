package pool

import (
	"errors"
	"time"

	"github.com/lysu/thriftpool-go/circuit"
)

var (
	// ErrClosed is error that pool closed
	ErrClosed = errors.New("resource pool is closed")
	// ErrTimeout is error that get resource timeout
	ErrTimeout = errors.New("resource pool timed out")
)

// ResourceFactory is an function to build single address resource
type ResourceFactory func(address string, pool Pool) (Resource, error)

// Resource is item can put into pool
// It can close and have address
type Resource interface {
	Close() error
	RawClose() error
	Address() string
	CircuitBreaker() (*circuit.CircuitBreaker, error)
	SetCircuitBreaker(circuitBreaker *circuit.CircuitBreaker)
}

// resourceWrapper is resource with usedTime
// pool use usedTime to idle them
type resourceWrapper struct {
	resource Resource
	timeUsed time.Time
}
