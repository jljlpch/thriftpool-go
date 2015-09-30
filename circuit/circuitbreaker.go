package circuit

import (
	"errors"
	"math"
	"net"
	"sync/atomic"
	"time"

	"github.com/lysu/thriftpool-go/log"

	"golang.org/x/net/context"
)

// ErrorReporter present reporter that can report error
type ErrorReporter interface {
	ReportError(context.Context, error) bool
	ReportResult(ctx context.Context, success bool)
}

// CircuitBreaker use to metric and break request when many failure
type CircuitBreaker struct {
	Addr           string
	failCount      uint64
	openTime       time.Time
	timeToOpen     time.Duration // time to dead
	TimeToOpenUnit time.Duration // min time unit for ttd
	MaxTimeToOpen  time.Duration // max time for ttd
}

// BreakerGroup manage a group of resource breaker
type BreakerGroup struct {
	Breakers []*CircuitBreaker
	counter  *uint64
}

// NewCircuitBreaker use to create CircuitBreaker
func NewCircuitBreaker(addr string, ttoUnit, ttoMax time.Duration) *CircuitBreaker {
	return &CircuitBreaker{Addr: addr, TimeToOpenUnit: ttoUnit, MaxTimeToOpen: ttoMax}
}

// NewBreakerGroup use to create list of group status
func NewBreakerGroup() *BreakerGroup {
	sl := &BreakerGroup{}
	sl.counter = new(uint64)
	*sl.counter = 0
	return sl
}

// ReportError record error call result
func (s *CircuitBreaker) ReportError(ctx context.Context, err error) (success bool) {
	success = !IsNetErr(err)
	s.ReportResult(ctx, success)
	return success
}

// ReportResult both success and failure result
// synchronized? no need!
func (s *CircuitBreaker) ReportResult(ctx context.Context, success bool) {
	if success {
		s.closeBreaker()
		return
	}
	// failed
	s.failCount++
	// cal ttd by failcount
	if s.failCount > 15 {
		s.timeToOpen = s.MaxTimeToOpen
		s.openBreaker()
	} else if s.failCount > 10 {
		s.timeToOpen = 4 * s.TimeToOpenUnit
		s.openBreaker()
	} else if s.failCount > 5 {
		s.timeToOpen = 2 * s.TimeToOpenUnit
		s.openBreaker()
	} else if s.failCount > 2 {
		s.timeToOpen = s.TimeToOpenUnit
		s.openBreaker()
	}
	// check if ttd exceeds max ttd
	if s.timeToOpen > s.MaxTimeToOpen {
		s.timeToOpen = s.MaxTimeToOpen
	}
	if s.isOpen() {
		log.Errorf(ctx, "resource is dead, failCount:%d, ttd: %v, resource: %v", s.failCount, s.timeToOpen, s.Addr)
	}
}

// GetAvailable get av closed breaker from group status
func (gm *BreakerGroup) GetAvailable(ctx context.Context) (*CircuitBreaker, error) {
	c := atomic.AddUint64(gm.counter, 1)
	length := len(gm.Breakers)
	idx := int(math.Abs(float64(c % uint64(length))))
	ss := gm.Breakers[idx]
	if !ss.isOpen() {
		return ss, nil
	}
	// failed, find next alive
	for i := 0; i < length-1; i++ {
		idx++
		ss := gm.Breakers[idx%length]
		if ss.isOpen() {
			// dead
			continue
		}
		return ss, nil
	}
	log.Errorf(ctx, "all pools were dead: %v ", gm.printAddrs())
	return nil, errors.New("All pools were dead")
}

// AddBreaker use to add breaker to BreakerGroup
func (gm *BreakerGroup) AddBreaker(b *CircuitBreaker) {
	gm.Breakers = append(gm.Breakers, b)
}

func (s *CircuitBreaker) isOpen() bool {
	return s.timeToOpen > 0 && time.Now().Sub(s.openTime) < s.timeToOpen
}

func (s *CircuitBreaker) openBreaker() {
	s.openTime = time.Now()
}

func (s *CircuitBreaker) closeBreaker() {
	s.failCount = 0
	s.timeToOpen = 0
	s.openTime = time.Time{}
}

func (gm *BreakerGroup) printAddrs() string {
	var ret string
	for _, ss := range gm.Breakers {
		ret = ret + ss.Addr
	}
	return ret
}

func IsNetErr(err error) bool {
	if err == nil {
		return false
	}
	/*if neterr, ok := err.(net.Error); ok && neterr.Timeout() { */
	if _, ok := err.(net.Error); ok {
		return true
	}
	return false
}
