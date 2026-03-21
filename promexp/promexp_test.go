package promexp

import (
	"testing"
	"time"

	"github.com/automixer/gobi/producer"
	"github.com/wow-look-at-my/testify/assert"
	"github.com/wow-look-at-my/testify/require"
	"github.com/prometheus/client_golang/prometheus"
)

func makeFlow(fields map[string]string, bytes, packets uint64) producer.Flow {
	return producer.Flow{
		Fields:		fields,
		Bytes:		bytes,
		Packets:	packets,
		TimeRcvd:	time.Now(),
	}
}

func newTestGobiProm(labelSet []string) *GobiProm {
	fc := &GobiProm{
		metricsName:	"test",
		labelSet:	labelSet,
		flowLife:	5 * time.Minute,
		maxScrapeInt:	2 * time.Minute,
		fTable:		make(flowTable, fTableInitSize),
		spinB:		newSpinBuff(2 * time.Minute),
		bytesDesc:	prometheus.NewDesc("test_bytes", "", labelSet, nil),
		packetsDesc:	prometheus.NewDesc("test_packets", "", labelSet, nil),
		ubytesDesc:	prometheus.NewDesc("test_untracked_bytes", "", nil, nil),
		upacketsDesc:	prometheus.NewDesc("test_untracked_packets", "", nil, nil),
		lastScrape:	time.Now(),
	}
	return fc
}

func TestAggregateFlows(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})

	flows := []producer.Flow{
		makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 100, 1),
		makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 200, 2),
		makeFlow(map[string]string{"srcaddr": "10.0.0.2"}, 300, 3),
	}

	table := g.aggregateFlows(flows)

	require.Equal(t, 2, len(table))

	// 10.0.0.1 should be aggregated: 100+200=300 bytes, 1+2=3 packets
	f1 := table["10.0.0.1"]
	assert.Equal(t, uint64(300), f1.Bytes)

	assert.Equal(t, uint64(3), f1.Packets)

	f2 := table["10.0.0.2"]
	assert.Equal(t, uint64(300), f2.Bytes)

}

func TestAggregateFlowsMultiLabel(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr", "dstaddr"})

	flows := []producer.Flow{
		makeFlow(map[string]string{"srcaddr": "10.0.0.1", "dstaddr": "10.0.0.2"}, 100, 1),
		makeFlow(map[string]string{"srcaddr": "10.0.0.1", "dstaddr": "10.0.0.2"}, 200, 2),
		makeFlow(map[string]string{"srcaddr": "10.0.0.1", "dstaddr": "10.0.0.3"}, 50, 1),
	}

	table := g.aggregateFlows(flows)
	require.Equal(t, 2, len(table))

}

func TestMergeToMainTable(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})

	// Pre-populate main table
	g.fTable["10.0.0.1"] = makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 100, 1)

	newTable := flowTable{
		"10.0.0.1":	makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 200, 2),
		"10.0.0.2":	makeFlow(map[string]string{"srcaddr": "10.0.0.2"}, 300, 3),
	}

	g.mergeToMainTable(newTable)

	require.Equal(t, 2, len(g.fTable))

	assert.Equal(t, uint64(300), g.fTable["10.0.0.1"].Bytes)

	assert.Equal(t, uint64(300), g.fTable["10.0.0.2"].Bytes)

}

func TestPruneUnderRate(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	g.minBps = 100	// 100 bps minimum
	g.minPps = 0

	ft := flowTable{
		"high":	makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 1000, 10),	// 8000 bps over 10s
		"low":	makeFlow(map[string]string{"srcaddr": "10.0.0.2"}, 1, 1),	// 0.8 bps over 10s
	}

	g.pruneUnderRate(&ft, 10)	// 10 second interval

	require.Equal(t, 1, len(ft))

	_, ok := ft["high"]
	assert.True(t, ok)

	assert.Equal(t, uint64(1), g.ubytesCnt)

	assert.Equal(t, uint64(1), g.upacketsCnt)

}

func TestPruneUnderRateZeroInterval(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	g.minBps = 100

	ft := flowTable{
		"a": makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 1000, 10),
	}

	g.pruneUnderRate(&ft, 0)	// zero interval => rate is 0

	assert.Equal(t, 0, len(ft))

}

func TestPruneUnderRatePps(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	g.minBps = 0
	g.minPps = 5

	ft := flowTable{
		"high":	makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 1000, 100),	// 10 pps over 10s
		"low":	makeFlow(map[string]string{"srcaddr": "10.0.0.2"}, 1000, 10),	// 1 pps over 10s
	}

	g.pruneUnderRate(&ft, 10)

	require.Equal(t, 1, len(ft))

	_, ok := ft["high"]
	assert.True(t, ok)

}

func TestConsume(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	f := makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 100, 1)

	g.Consume(f)

	// Check it ended up in the spin buffer
	flows := g.spinB.checkOut()
	require.Equal(t, 1, len(flows))

	assert.Equal(t, uint64(100), flows[0].Bytes)

}

func TestDescribe(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	ch := make(chan *prometheus.Desc, 4)
	g.Describe(ch)
	close(ch)

	count := 0
	for range ch {
		count++
	}
	assert.Equal(t, 4, count)

}

func TestCollect(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	g.lastScrape = time.Now().Add(-10 * time.Second)

	// Add flows to spin buffer
	g.spinB.addFlow(makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 1000, 10))
	g.spinB.addFlow(makeFlow(map[string]string{"srcaddr": "10.0.0.2"}, 2000, 20))

	ch := make(chan prometheus.Metric, 20)
	g.Collect(ch)
	close(ch)

	count := 0
	for range ch {
		count++
	}
	// 2 untracked + 2 flows * 2 metrics each = 6
	assert.Equal(t, 6, count)

}

func TestCollectWithExpiredFlows(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	g.flowLife = 1 * time.Millisecond
	g.lastScrape = time.Now().Add(-1 * time.Second)

	// Add an old flow directly to the main table
	oldFlow := makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 1000, 10)
	oldFlow.TimeRcvd = time.Now().Add(-1 * time.Hour)
	g.fTable["10.0.0.1"] = oldFlow

	ch := make(chan prometheus.Metric, 20)
	g.Collect(ch)
	close(ch)

	// Should have been evicted
	assert.Equal(t, 0, len(g.fTable))

}

func TestCollectWithMinRate(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	g.minBps = 1000
	g.lastScrape = time.Now().Add(-10 * time.Second)

	// Add a low-rate flow
	g.spinB.addFlow(makeFlow(map[string]string{"srcaddr": "10.0.0.1"}, 1, 1))

	ch := make(chan prometheus.Metric, 20)
	g.Collect(ch)
	close(ch)

	// Flow should be pruned, only untracked metrics remain
	assert.Equal(t, 0, len(g.fTable))

	assert.Equal(t, uint64(1), g.ubytesCnt)

}

func TestSpinBuffAddAndCheckout(t *testing.T) {
	sb := newSpinBuff(1 * time.Minute)

	sb.addFlow(makeFlow(nil, 100, 1))
	sb.addFlow(makeFlow(nil, 200, 2))

	flows := sb.checkOut()
	require.Equal(t, 2, len(flows))

	// After checkout, should be empty
	flows = sb.checkOut()
	assert.Equal(t, 0, len(flows))

}

func TestSpinBuffFlush(t *testing.T) {
	sb := newSpinBuff(1 * time.Minute)

	sb.addFlow(makeFlow(nil, 100, 1))
	sb.flush()

	// After flush, the selector rotated and old buffer cleared
	flows := sb.checkOut()
	assert.Equal(t, 0, len(flows))

}

func TestAggregateFlowsEmpty(t *testing.T) {
	g := newTestGobiProm([]string{"srcaddr"})
	table := g.aggregateFlows(nil)
	assert.Equal(t, 0, len(table))

}
