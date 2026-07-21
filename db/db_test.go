package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFindDirection(t *testing.T) {
	g := &GobiDb{}
	tests := []struct {
		dir  uint32
		want string
	}{
		{0, "in"},
		{1, "out"},
		{2, msgNotAv},
		{99, msgNotAv},
	}
	for _, tt := range tests {
		got := g.FindDirection(tt.dir)
		assert.Equal(t, tt.want, got)

	}
}

func TestFindProto(t *testing.T) {
	g := &GobiDb{}

	// TCP = 6
	got := g.FindProto(6)
	assert.Equal(t, "tcp", got)

	// UDP = 17
	got = g.FindProto(17)
	assert.Equal(t, "udp", got)

	// Unknown protocol
	got = g.FindProto(254)
	assert.Equal(t, msgNotStd, got)

}

func TestFindProtoNoName(t *testing.T) {
	g := &GobiDb{NoProtoName: true}
	got := g.FindProto(6)
	assert.Equal(t, "6", got)

}

func TestFindSvc(t *testing.T) {
	g := &GobiDb{}

	// TCP port 80 = http
	got := g.FindSvc(6, 80)
	assert.Equal(t, "http", got)

	// UDP port 53 = domain
	got = g.FindSvc(17, 53)
	assert.Equal(t, "domain", got)

	// Non-TCP/UDP protocol
	got = g.FindSvc(1, 80)
	assert.Equal(t, msgNotStd, got)

	// Unknown TCP port
	got = g.FindSvc(6, 59999)
	assert.Equal(t, msgNotStd, got)

	// Unknown UDP port
	got = g.FindSvc(17, 59999)
	assert.Equal(t, msgNotStd, got)

}

func TestFindSvcNoPortName(t *testing.T) {
	g := &GobiDb{NoPortName: true}
	got := g.FindSvc(6, 80)
	assert.Equal(t, "80", got)

}

func TestFindEtype(t *testing.T) {
	g := &GobiDb{}
	tests := []struct {
		etype uint32
		want  string
	}{
		{0x0800, "IPv4"},
		{0x0806, "ARP"},
		{0x8100, "802.1q"},
		{0x86dd, "IPv6"},
		{0x8809, "Slow Protocols"},
		{0x8847, "MPLS Unicast"},
		{0x8848, "MPLS Multicast"},
		{0x8863, "PPPoE Discovery"},
		{0x8864, "PPPoE Session"},
		{0x88a8, "QinQ"},
		{0x88cc, "LLDP"},
		{0x88e5, "MACsec"},
		{0x88e7, "PBB"},
		{0x88f7, "PTP"},
		{0x8906, "FCoE"},
		{0x9999, msgNotStd},
	}
	for _, tt := range tests {
		got := g.FindEtype(tt.etype)
		assert.Equal(t, tt.want, got)

	}
}

func TestFindEtypeNoName(t *testing.T) {
	g := &GobiDb{NoEtypeName: true}
	got := g.FindEtype(0x0800)
	assert.Equal(t, "0x800", got)

}

func TestFindIpAddr(t *testing.T) {
	g := &GobiDb{}

	// IPv4
	got := g.FindIpAddr([]byte{192, 168, 1, 1})
	assert.Equal(t, "192.168.1.1", got)

	// IPv6
	got = g.FindIpAddr([]byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01})
	assert.Equal(t, "2001:db8::1", got)

	// Nil
	got = g.FindIpAddr(nil)
	assert.Equal(t, msgNotAv, got)

}

func TestFindNetwork(t *testing.T) {
	g := &GobiDb{}

	got := g.FindNetwork([]byte{10, 0, 0, 1}, 24)
	assert.Equal(t, "10.0.0.0/24", got)

	got = g.FindNetwork([]byte{172, 16, 5, 100}, 16)
	assert.Equal(t, "172.16.0.0/16", got)

	// Invalid
	got = g.FindNetwork(nil, 24)
	assert.Equal(t, "invalid", got)

}

func TestFindASN(t *testing.T) {
	g := &GobiDb{}

	// Without MaxMind DB, should return formatted AS number
	got := g.FindASN([]byte{8, 8, 8, 8}, 15169)
	assert.Equal(t, "AS15169", got)

	got = g.FindASN([]byte{1, 1, 1, 1}, 0)
	assert.Equal(t, "AS0", got)

}

func TestFindCountry(t *testing.T) {
	g := &GobiDb{}

	// Without MaxMind DB, should return "ZZ"
	got := g.FindCountry([]byte{8, 8, 8, 8})
	assert.Equal(t, "ZZ", got)

}

func TestOpenCloseDbs(t *testing.T) {
	g := &GobiDb{}

	// Open with empty paths (no error, just no-ops)
	g.OpenDbs("", "")
	assert.Nil(t, g.maxMindASN)

	assert.Nil(t, g.maxMindCountry)

	// Open with invalid paths
	g.OpenDbs("invalid.mmdb", "invalid.mmdb")
	assert.Nil(t, g.maxMindASN)

	assert.Nil(t, g.maxMindCountry)

	// Open with unsupported format
	g.OpenDbs("something.json", "something.json")
	assert.Nil(t, g.maxMindASN)

	// Close is safe on nil
	g.CloseDbs()
}
