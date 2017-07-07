package mdata

import (
	"fmt"
	"sync"

	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/mdata/cache"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/worldping-api/pkg/log"
)

// AggMetric takes in new values, updates the in-memory data and streams the points to aggregators
// it uses a circular buffer of chunks
// each chunk starts at their respective t0
// a t0 is a timestamp divisible by chunkSpan without a remainder (e.g. 2 hour boundaries)
// firstT0's data is held at index 0, indexes go up and wrap around from numChunks-1 to 0
// in addition, keep in mind that the last chunk is always a work in progress and not useable for aggregation
// AggMetric is concurrency-safe
type AggMetric struct {
	sync.RWMutex

	native      *ChunkKeeper
	aggregators []*Aggregator
}

// NewAggMetric creates a metric with given key, it retains the given number of chunks each chunkSpan seconds long
// it optionally also creates aggregations with the given settings
// the 0th retention is the native archive of this metric. if there's several others, we create aggregators, using agg.
// it's the callers responsibility to make sure agg is not nil in that case!
func NewAggMetric(store Store, cachePusher cache.CachePusher, key string, retentions conf.Retentions, agg *conf.Aggregation, dropFirstChunk bool) *AggMetric {

	// note: during parsing of retentions, we assure there's at least 1.
	ret := retentions[0]

	m := AggMetric{}

	m.native = NewChunkKeeper(&m, store, cachePusher, key, ret, dropFirstChunk)
	for _, ret := range retentions[1:] {
		m.aggregators = append(m.aggregators, NewAggregator(store, cachePusher, key, ret, *agg, dropFirstChunk))
	}

	return &m
}

// Sync the saved state of a chunk by its T0.
func (a *AggMetric) SyncChunkSaveState(ts uint32) {
	a.Lock()
	defer a.Unlock()
	ck := a.native
	if ts > ck.lastSaveFinish {
		ck.lastSaveFinish = ts
	}
	if ts > ck.lastSaveStart {
		ck.lastSaveStart = ts
	}
	if LogLevel < 2 {
		log.Debug("AM metric %s at chunk T0=%d has been saved.", ck.Key, ts)
	}
}

// Sync the saved state of a chunk by its T0.
func (a *AggMetric) SyncAggregatedChunkSaveState(ts uint32, consolidator consolidation.Consolidator, aggSpan uint32) {
	// no lock needed cause aggregators don't change at runtime
	for _, a := range a.aggregators {
		if a.span == aggSpan {
			switch consolidator {
			case consolidation.None:
				panic("cannot get an archive for no consolidation")
			case consolidation.Avg:
				panic("avg consolidator has no matching Archive(). you need sum and cnt")
			case consolidation.Cnt:
				if a.cntMetric != nil {
					a.cntMetric.SyncChunkSaveState(ts)
				}
				return
			case consolidation.Min:
				if a.minMetric != nil {
					a.minMetric.SyncChunkSaveState(ts)
				}
				return
			case consolidation.Max:
				if a.maxMetric != nil {
					a.maxMetric.SyncChunkSaveState(ts)
				}
				return
			case consolidation.Sum:
				if a.sumMetric != nil {
					a.sumMetric.SyncChunkSaveState(ts)
				}
				return
			case consolidation.Lst:
				if a.lstMetric != nil {
					a.lstMetric.SyncChunkSaveState(ts)
				}
				return
			default:
				panic(fmt.Sprintf("internal error: no such consolidator %q with span %d", consolidator, aggSpan))
			}
		}
	}
}

func (a *AggMetric) GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (uint32, []chunk.Iter) {
	// no lock needed cause aggregators don't change at runtime
	for _, a := range a.aggregators {
		if a.span == aggSpan {
			switch consolidator {
			case consolidation.None:
				panic("cannot get an archive for no consolidation")
			case consolidation.Avg:
				panic("avg consolidator has no matching Archive(). you need sum and cnt")
			case consolidation.Cnt:
				return a.cntMetric.Get(from, to)
			case consolidation.Lst:
				return a.lstMetric.Get(from, to)
			case consolidation.Min:
				return a.minMetric.Get(from, to)
			case consolidation.Max:
				return a.maxMetric.Get(from, to)
			case consolidation.Sum:
				return a.sumMetric.Get(from, to)
			}
			panic(fmt.Sprintf("AggMetric.GetAggregated(): unknown consolidator %q", consolidator))
		}
	}
	panic(fmt.Sprintf("GetAggregated called with unknown aggSpan %d", aggSpan))
}

// Get all data between the requested time ranges. From is inclusive, to is exclusive. from <= x < to
// more data then what's requested may be included
// also returns oldest point we have, so that if your query needs data before it, the caller knows when to query cassandra
func (a *AggMetric) Get(from, to uint32) (uint32, []chunk.Iter) {
	return a.native.Get(from, to)
}

// this function must only be called while holding the lock
func (a *AggMetric) addAggregators(ts uint32, val float64) {
	for _, agg := range a.aggregators {
		if LogLevel < 2 {
			log.Debug("AM %s pushing %d,%f to aggregator %d", a.native.Key, ts, val, agg.span)
		}
		agg.Add(ts, val)
	}
}

// don't ever call with a ts of 0, cause we use 0 to mean not initialized!
func (a *AggMetric) Add(ts uint32, val float64) {
	a.Lock()
	defer a.Unlock()
	a.native.Add(ts, val)
	a.addAggregators(ts, val)
}

func (a *AggMetric) GC(chunkMinTs, metricMinTs uint32) bool {
	a.Lock()
	defer a.Unlock()

	return a.native.GC(chunkMinTs, metricMinTs)
}
