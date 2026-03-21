package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/automixer/gobi/producer"
	"github.com/wow-look-at-my/testify/assert"
	"github.com/wow-look-at-my/testify/require"
	"github.com/automixer/gobi/promexp"
)

func TestFormatLabelSet(t *testing.T) {
	tests := []struct {
		name	string
		in	[]string
		want	[]string
	}{
		{
			name:	"lowercase",
			in:	[]string{"SrcAddr", "DstAddr"},
			want:	[]string{"srcaddr", "dstaddr"},
		},
		{
			name:	"deduplicate",
			in:	[]string{"SrcAddr", "SrcAddr", "DstAddr"},
			want:	[]string{"srcaddr", "dstaddr"},
		},
		{
			name:	"empty",
			in:	[]string{},
			want:	[]string{},
		},
		{
			name:	"single",
			in:	[]string{"Proto"},
			want:	[]string{"proto"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatLabelSet(tt.in)
			assert.Equal(t, got, tt.want)

		})
	}
}

func TestLoadCfgFromFileDefaults(t *testing.T) {
	cfg := loadCfgFromFile("nonexistent.yml")

	assert.Equal(t, ":9310", cfg.Global.MetricsAddr)

	assert.Equal(t, "/metrics", cfg.Global.MetricsPath)

	assert.Equal(t, "stdin", cfg.Producer.Input)

	assert.True(t, cfg.Producer.Normalize)

	assert.Equal(t, -1, cfg.Producer.SrOverride)

}

func TestLoadCfgFromFileValid(t *testing.T) {
	content := `
global:
  metricsaddr: ":8080"
  metricspath: "/prom"

producer:
  input: stdin
  normalize: false

promexporters:
  - metricsname: test
    labelset: ["SrcAddr", "DstAddr"]
    flowlife: "10m"
    maxscrapeint: "5m"
    minbps: 100
    minpps: 10
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.yml")
	require.NoError(t, os.WriteFile(tmpFile, []byte(content), 0o644))

	cfg := loadCfgFromFile(tmpFile)

	assert.Equal(t, ":8080", cfg.Global.MetricsAddr)

	assert.Equal(t, "/prom", cfg.Global.MetricsPath)

	require.Equal(t, 1, len(cfg.Promexporters))

	assert.Equal(t, "test", cfg.Promexporters[0].MetricsName)

}

func TestLoadCfgFromFileInvalidYaml(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "bad.yml")
	require.NoError(t, os.WriteFile(tmpFile, []byte("{{invalid yaml"), 0o644))

	// Should not panic, just use defaults
	cfg := loadCfgFromFile(tmpFile)
	assert.Equal(t, ":9310", cfg.Global.MetricsAddr)

}

func TestParseExpCfg(t *testing.T) {
	yCfg := appConfig{
		Promexporters: []promexp.Config{
			{
				MetricsName:	"myexp",
				MinBps:		100,
				MinPps:		10,
				FlowLife:	"10m",
				MaxScrapeInt:	"3m",
				LabelSet:	[]string{"SrcAddr", "DstAddr"},
			},
		},
	}

	result := parseExpCfg(yCfg)

	require.Equal(t, 1, len(result))

	assert.Equal(t, "gobi_myexp", result[0].MetricsName)

	assert.Equal(t, "10m", result[0].FlowLife)

	assert.Equal(t, "3m", result[0].MaxScrapeInt)

	assert.Equal(t, uint64(100), result[0].MinBps)

}

func TestParseExpCfgDefaults(t *testing.T) {
	yCfg := appConfig{
		Promexporters: []promexp.Config{
			{},	// empty config, should get defaults
		},
	}

	result := parseExpCfg(yCfg)

	assert.Equal(t, "pexp0", result[0].MetricsName)

	assert.Equal(t, "5m", result[0].FlowLife)

	assert.Equal(t, "2m", result[0].MaxScrapeInt)

	assert.Equal(t, result[0].LabelSet, []string{"SamplerAddress"})

}

func TestParseExpCfgInvalidDurations(t *testing.T) {
	yCfg := appConfig{
		Promexporters: []promexp.Config{
			{
				FlowLife:	"invalid",
				MaxScrapeInt:	"also-invalid",
			},
		},
	}

	result := parseExpCfg(yCfg)

	// Should keep defaults when durations are invalid
	assert.Equal(t, "5m", result[0].FlowLife)

	assert.Equal(t, "2m", result[0].MaxScrapeInt)

}

func TestParseExpCfgMultiple(t *testing.T) {
	yCfg := appConfig{
		Promexporters: []promexp.Config{
			{MetricsName: "first"},
			{MetricsName: "second"},
		},
	}

	result := parseExpCfg(yCfg)

	require.Equal(t, 2, len(result))

	assert.Equal(t, "gobi_first", result[0].MetricsName)

	assert.Equal(t, "gobi_second", result[1].MetricsName)

}

func TestParseGlobalCfg(t *testing.T) {
	yCfg := appConfig{
		Global: Config{
			MetricsAddr:	":9310",
			MetricsPath:	"/metrics",
			CreateFifo:	false,
		},
		Producer: producer.Config{
			Input: "stdin",
		},
	}

	result := parseGlobalCfg(yCfg)
	assert.Equal(t, ":9310", result.MetricsAddr)

}

func TestParseGlobalCfgCreateFifoStdin(t *testing.T) {
	// CreateFifo=true but input=stdin should not create fifo
	yCfg := appConfig{
		Global: Config{
			CreateFifo: true,
		},
		Producer: producer.Config{
			Input: "stdin",
		},
	}

	result := parseGlobalCfg(yCfg)
	assert.Equal(t, true, result.CreateFifo)

}

func TestParseGlobalCfgCreateFifo(t *testing.T) {
	tmpDir := t.TempDir()
	fifoPath := filepath.Join(tmpDir, "testfifo")

	yCfg := appConfig{
		Global: Config{
			CreateFifo: true,
		},
		Producer: producer.Config{
			Input: fifoPath,
		},
	}

	parseGlobalCfg(yCfg)

	// Check fifo was created
	info, err := os.Stat(fifoPath)
	require.Nil(t, err)

	assert.NotEqual(t, 0, info.Mode()&os.ModeNamedPipe)

}

func TestNewAppConfig(t *testing.T) {
	cfg := newAppConfig("nonexistent.yml")
	assert.Equal(t, ":9310", cfg.Global.MetricsAddr)

}
