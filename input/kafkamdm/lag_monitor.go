package kafkamdm

import (
	"sync"
	"time"
)

type lagLogger struct {
	sync.Mutex
	pos          int
	measurements []int
}

func newLagLogger(size int) *lagLogger {
	return &lagLogger{
		pos:          0,
		measurements: make([]int, 0, size),
	}
}

func (l *lagLogger) Store(lag int) {
	l.Lock()
	defer l.Unlock()
	l.pos++
	if len(l.measurements) < cap(l.measurements) {
		l.measurements = append(l.measurements, lag)
		return
	}

	if l.pos >= cap(l.measurements) {
		l.pos = 0
	}
	l.measurements[l.pos] = lag
}

func (l *lagLogger) Min() int {
	l.Lock()
	defer l.Unlock()
	min := -1
	for _, m := range l.measurements {
		if min < 0 || m < min {
			min = m
		}
	}
	if min < 0 {
		min = 0
	}
	return min
}

type rateLogger struct {
	sync.Mutex
	lastOffset int64
	lastTs     time.Time
	rate       int64
}

func (o *rateLogger) Store(offset int64, ts time.Time) {
	o.Lock()
	defer o.Unlock()
	if o.lastTs.IsZero() {
		// first measurement
		o.lastOffset = offset
		o.lastTs = ts
		return
	}
	duration := ts.Sub(o.lastTs)
	if duration < time.Second && duration > 0 {
		// too small difference. either due to clock adjustment or this method
		// is called very frequently, e.g. due to a subsecond offset-commit-interval.
		// We need to let more time pass to make an accurate calculation.
		return
	}
	if duration <= 0 {
		// current ts is <= last ts. This would only happen if clock went back in time
		// in which case we can't reliably work out how
		// long it has really been since we last took a measurement.
		// but set a new baseline for next time
		o.lastTs = ts
		o.lastOffset = offset
		return
	}
	metrics := offset - o.lastOffset
	o.lastTs = ts
	o.lastOffset = offset
	if metrics < 0 {
		// this is possible if our offset counter rolls over or is reset.
		// If it was a rollover we could compute the rate, but it is safer
		// to just keep using the last computed rate, and wait for the next
		// measurement to compute a new rate based on the new baseline
		return
	}
	// note the multiplication overflows if you have 9 billion metrics
	// sice max int64 is 9 223 372 036 854 775 807 (not an issue in practice)
	o.rate = (1e9 * metrics / int64(duration)) // metrics/ns -> metrics/s
	return
}

func (o *rateLogger) Rate() int64 {
	o.Lock()
	defer o.Unlock()
	return o.rate
}

func newRateLogger() *rateLogger {
	return &rateLogger{}
}

/*
   LagMonitor is used to determine how upToDate this node is.
   We periodically collect the lag for each partition, keeping the last N
   measurements in a moving window. We also collect the ingest rate of each
   partition. Using these measurements we can then compute a overall score
   for this each partition. The score is just the minimum lag seen in the last
   N measurements divided by the ingest rate. So if the ingest rate is 1k/second
   and the lag is 10000 messages. Then our reported lag is 10, meaning 10seconds.
   If the rate is 1k/second and the lag is 200 then our lag is reported as 0, meaning
   the node is less then 1second behind.

   The node's overall lag is then the highest lag of all partitions.
*/
type LagMonitor struct {
	lag  map[int32]*lagLogger
	rate map[int32]*rateLogger
}

func NewLagMonitor(size int, partitions []int32) *LagMonitor {
	m := &LagMonitor{
		lag:  make(map[int32]*lagLogger),
		rate: make(map[int32]*rateLogger),
	}
	for _, p := range partitions {
		m.lag[p] = newLagLogger(size)
		m.rate[p] = newRateLogger()
	}
	return m
}

func (l *LagMonitor) Metric() int {
	max := 0
	for p, lag := range l.lag {
		rate := l.rate[p]
		l := lag.Min()
		r := rate.Rate()
		if r == 0 {
			r = 1
		}
		val := l / int(r)
		if val > max {
			max = val
		}
	}
	return max
}

func (l *LagMonitor) StoreLag(partition int32, val int) {
	l.lag[partition].Store(val)
}

func (l *LagMonitor) StoreOffset(partition int32, offset int64, ts time.Time) {
	l.rate[partition].Store(offset, ts)
}
