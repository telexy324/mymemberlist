package memberlist

import (
	"math"
	"sync/atomic"
	"time"
)

// suspicion manages the suspect timer for a node and provides an interface
// to accelerate the timeout as we get more independent confirmations that
// a node is suspect.
type suspicion struct { //本质是一个定时器，收集到k个suspect信息后，就触发timeoutFunc，或者超时了也触发timeoutFunc
	// n is the number of independent confirmations we've seen. This must
	// be updated using atomic instructions to prevent contention with the
	// timer callback.
	n int32 //表示收到多少该节点处于suspect的消息

	// k is the number of independent confirmations we'd like to see in
	// order to drive the timer to its minimum value.
	k int32 //表示达到多少个suspect确认消息后就把timer置为最小值

	// min is the minimum timer value.
	min time.Duration //如果没有设置k，则定时器的到期时间取这个值

	// max is the maximum timer value.
	max time.Duration //如果设置了k，则定时器到期时间取这个值

	// start captures the timestamp when we began the timer. This is used
	// so we can calculate durations to feed the timer during updates in
	// a way the achieves the overall time we'd like.
	start time.Time

	// timer is the underlying timer that implements the timeout.
	timer *time.Timer

	// f is the function to call when the timer expires. We hold on to this
	// because there are cases where we call it directly.
	timeoutFn func()

	// confirmations is a map of "from" nodes that have confirmed a given
	// node is suspect. This prevents double counting.
	confirmations map[string]struct{} //储存收到suspect的来源节点，防止重复
}

// newSuspicion returns a timer started with the max time, and that will drive
// to the min time after seeing k or more confirmations. The from node will be
// excluded from confirmations since we might get our own suspicion message
// gossiped back to us. The minimum time will be used if no confirmations are
// called for (k <= 0).
func newSuspicion(from string, k int, min time.Duration, max time.Duration, fn func(int)) *suspicion {
	s := &suspicion{
		k:             int32(k),
		min:           min,
		max:           max,
		confirmations: make(map[string]struct{}),
	}

	// Exclude the from node from any confirmations.
	s.confirmations[from] = struct{}{} //预置from节点，以后此节点发来的都忽略

	// Pass the number of confirmations into the timeout function for
	// easy telemetry.
	s.timeoutFn = func() { //这样写相当于回调函数就是fn
		fn(int(atomic.LoadInt32(&s.n))) //fn要求传入的参数是已经有多少个确认数
	}

	// If there aren't any confirmations to be made then take the min
	// time from the start.
	timeout := max
	if k < 1 {
		timeout = min
	}
	s.timer = time.AfterFunc(timeout, s.timeoutFn)

	// Capture the start time right after starting the timer above so
	// we should always err on the side of a little longer timeout if
	// there's any preemption that separates this and the step above.
	s.start = time.Now()
	return s
}

// remainingSuspicionTime takes the state variables of the suspicion timer and
// calculates the remaining time to wait before considering a node dead. The
// return value can be negative, so be prepared to fire the timer immediately in
// that case. 计算还剩多少时间，时间是负的马上触发timeout，怎么算的没看懂
func remainingSuspicionTime(n, k int32, elapsed time.Duration, min, max time.Duration) time.Duration {
	frac := math.Log(float64(n)+1.0) / math.Log(float64(k)+1.0)
	raw := max.Seconds() - frac*(max.Seconds()-min.Seconds())
	timeout := time.Duration(math.Floor(1000.0*raw)) * time.Millisecond
	if timeout < min {
		timeout = min
	}

	// We have to take into account the amount of time that has passed so
	// far, so we get the right overall timeout.
	return timeout - elapsed
}

// Confirm registers that a possibly new peer has also determined the given
// node is suspect. This returns true if this was new information, and false
// if it was a duplicate confirmation, or if we've got enough confirmations to
// hit the minimum.
func (s *suspicion) Confirm(from string) bool {
	// If we've got enough confirmations then stop accepting them.收集到足够的就停
	if atomic.LoadInt32(&s.n) >= s.k {
		return false
	}

	// Only allow one confirmation from each possible peer.
	if _, ok := s.confirmations[from]; ok { //不接受重复确认
		return false
	}
	s.confirmations[from] = struct{}{}

	// Compute the new timeout given the current number of confirmations and
	// adjust the timer. If the timeout becomes negative *and* we can cleanly
	// stop the timer then we will call the timeout function directly from
	// here.
	n := atomic.AddInt32(&s.n, 1)
	elapsed := time.Since(s.start)
	remaining := remainingSuspicionTime(n, s.k, elapsed, s.min, s.max)
	if s.timer.Stop() { //停掉现有的timer,返回false就是已经停掉了
		if remaining > 0 {
			s.timer.Reset(remaining) //如果还没到时间，就加回去，等于是重新来了个timer
		} else {
			go s.timeoutFn()
		}
	}
	return true
}
