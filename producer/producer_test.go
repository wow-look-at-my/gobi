package producer

import (
	"bufio"
	"bytes"
	"syscall"
	"testing"
	"time"

	flowpb "github.com/netsampler/goflow2/v2/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protodelim"
)

type mockConsumer struct {
	flows []Flow
}

func (m *mockConsumer) Consume(flow Flow) {
	m.flows = append(m.flows, flow)
}

func TestNewFlow(t *testing.T) {
	g := &GobiGf2{}
	msg := &flowpb.FlowMessage{
		Type:           flowpb.FlowMessage_IPFIX,
		SamplerAddress: []byte{10, 0, 0, 1},
		SrcAddr:        []byte{192, 168, 1, 1},
		DstAddr:        []byte{10, 0, 0, 2},
		Etype:          0x0800,
		Proto:          6,
		SrcPort:        12345,
		DstPort:        80,
		InIf:           1,
		OutIf:          2,
		SrcAs:          64512,
		DstAs:          15169,
		NextHop:        []byte{10, 0, 0, 254},
		NextHopAs:      64512,
		SrcNet:         24,
		DstNet:         16,
		Bytes:          1000,
		Packets:        10,
		SamplingRate:   1,
	}

	f := g.newFlow(msg)

	assert.Equal(t, "IPFIX", f.Fields["type"])

	assert.Equal(t, "192.168.1.1", f.Fields["srcaddr"])

	assert.Equal(t, "10.0.0.2", f.Fields["dstaddr"])

	assert.Equal(t, "10.0.0.1", f.Fields["sampleraddress"])

	assert.Equal(t, "1", f.Fields["inif"])

	assert.Equal(t, "2", f.Fields["outif"])

	assert.Equal(t, uint64(1000), f.Bytes)

	assert.Equal(t, uint64(10), f.Packets)

	assert.False(t, f.TimeRcvd.IsZero())

}

func TestNewFlowNormalize(t *testing.T) {
	g := &GobiGf2{normalize: true, srOverride: -1}
	msg := &flowpb.FlowMessage{
		Bytes:        500,
		Packets:      5,
		SamplingRate: 100,
		SrcAddr:      []byte{1, 2, 3, 4},
		DstAddr:      []byte{5, 6, 7, 8},
		NextHop:      []byte{9, 10, 11, 12},
	}

	f := g.newFlow(msg)
	assert.Equal(t, uint64(50000), f.Bytes)

	assert.Equal(t, uint64(500), f.Packets)

}

func TestNewFlowNormalizeWithOverride(t *testing.T) {
	g := &GobiGf2{normalize: true, srOverride: 200}
	msg := &flowpb.FlowMessage{
		Bytes:        100,
		Packets:      1,
		SamplingRate: 50, // Will be overridden to 200
		SrcAddr:      []byte{1, 2, 3, 4},
		DstAddr:      []byte{5, 6, 7, 8},
		NextHop:      []byte{9, 10, 11, 12},
	}

	f := g.newFlow(msg)
	assert.Equal(t, uint64(20000), f.Bytes)

	assert.Equal(t, uint64(200), f.Packets)

}

func TestNewFlowNoNormalize(t *testing.T) {
	g := &GobiGf2{normalize: false}
	msg := &flowpb.FlowMessage{
		Bytes:        500,
		Packets:      5,
		SamplingRate: 100,
		SrcAddr:      []byte{1, 2, 3, 4},
		DstAddr:      []byte{5, 6, 7, 8},
		NextHop:      []byte{9, 10, 11, 12},
	}

	f := g.newFlow(msg)
	assert.Equal(t, uint64(500), f.Bytes)

}

func TestMsgRoutine(t *testing.T) {
	// Create a protobuf message and encode it with protodelim
	msg := &flowpb.FlowMessage{
		Type:    flowpb.FlowMessage_NETFLOW_V9,
		SrcAddr: []byte{10, 0, 0, 1},
		DstAddr: []byte{10, 0, 0, 2},
		NextHop: []byte{10, 0, 0, 254},
		Bytes:   2000,
		Packets: 20,
	}

	var buf bytes.Buffer
	_, err := protodelim.MarshalTo(&buf, msg)
	require.Nil(t, err)

	g := &GobiGf2{
		inRdr:  bufio.NewReader(&buf),
		flowCh: make(chan Flow, 1),
		doneCh: make(chan struct{}),
	}

	go g.msgRoutine()

	select {
	case f := <-g.flowCh:
		assert.Equal(t, "NETFLOW_V9", f.Fields["type"])

		assert.Equal(t, uint64(2000), f.Bytes)

	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for flow")
	}
}

func TestRegister(t *testing.T) {
	g := &GobiGf2{
		consumers: make([]Consumer, 0, maxConsumers),
		consCh:    make(chan Consumer, 1),
		doneCh:    make(chan struct{}),
		flowCh:    make(chan Flow),
	}
	go g.ctrlRoutine()

	mc := &mockConsumer{}
	err := g.Register(mc)
	assert.Nil(t, err)

	// Register nil should fail
	err = g.Register(nil)
	assert.NotNil(t, err)

}

func TestRegisterMaxConsumers(t *testing.T) {
	g := &GobiGf2{
		consumers: make([]Consumer, maxConsumers),
		consCh:    make(chan Consumer, 1),
	}

	err := g.Register(&mockConsumer{})
	assert.NotNil(t, err)

}

func TestCtrlRoutine(t *testing.T) {
	g := &GobiGf2{
		consumers: make([]Consumer, 0, maxConsumers),
		consCh:    make(chan Consumer, 1),
		doneCh:    make(chan struct{}),
		flowCh:    make(chan Flow, 1),
	}
	go g.ctrlRoutine()

	mc := &mockConsumer{}
	g.consCh <- mc

	// Give ctrlRoutine time to process
	time.Sleep(10 * time.Millisecond)

	testFlow := Flow{
		Fields:  map[string]string{"type": "test"},
		Bytes:   100,
		Packets: 1,
	}
	g.flowCh <- testFlow

	// Give ctrlRoutine time to process
	time.Sleep(10 * time.Millisecond)

	require.Equal(t, 1, len(mc.flows))

	assert.Equal(t, uint64(100), mc.flows[0].Bytes)

	close(g.doneCh)
}

func TestMsgRoutineMultiple(t *testing.T) {
	// Encode multiple messages
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		msg := &flowpb.FlowMessage{
			Type:    flowpb.FlowMessage_IPFIX,
			SrcAddr: []byte{10, 0, 0, byte(i + 1)},
			DstAddr: []byte{10, 0, 0, 100},
			NextHop: []byte{10, 0, 0, 254},
			Bytes:   uint64((i + 1) * 1000),
			Packets: uint64((i + 1) * 10),
		}
		_, err := protodelim.MarshalTo(&buf, msg)
		require.Nil(t, err)

	}

	g := &GobiGf2{
		inRdr:  bufio.NewReader(&buf),
		flowCh: make(chan Flow, 3),
		doneCh: make(chan struct{}),
	}

	go g.msgRoutine()

	for i := 0; i < 3; i++ {
		select {
		case f := <-g.flowCh:
			expectedBytes := uint64((i + 1) * 1000)
			assert.Equal(t, expectedBytes, f.Bytes)

		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for flow %d", i)
		}
	}
}

func TestNewStdin(t *testing.T) {
	cfg := Config{
		Input:       "stdin",
		Normalize:   true,
		SrOverride:  -1,
		NoPortName:  true,
		NoProtoName: true,
		NoEtypeName: true,
	}

	g, cleanup := New(cfg)
	defer cleanup()

	assert.NotNil(t, g)
	assert.NotNil(t, g.inRdr)
	assert.Nil(t, g.inPipe)
	assert.True(t, g.normalize)
	assert.Equal(t, -1, g.srOverride)
	assert.True(t, g.NoPortName)
	assert.True(t, g.NoProtoName)
	assert.True(t, g.NoEtypeName)
}

func TestNewNamedPipe(t *testing.T) {
	tmpDir := t.TempDir()
	fifoPath := tmpDir + "/testpipe"

	err := syscall.Mkfifo(fifoPath, 0o666)
	require.Nil(t, err)

	cfg := Config{
		Input:      fifoPath,
		Normalize:  false,
		SrOverride: 100,
	}

	g, cleanup := New(cfg)
	defer cleanup()

	assert.NotNil(t, g)
	assert.NotNil(t, g.inRdr)
	assert.NotNil(t, g.inPipe)
	assert.False(t, g.normalize)
	assert.Equal(t, 100, g.srOverride)
}
