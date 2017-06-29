// Package chreader reads metrics from cloudhealth
package chreader

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"time"
)

// An Entry represents all the cloudhealth metrics at a particular timestamp
type Entry struct {
	// The timestamp
	Time time.Time
	// Keys are the metric names values are the metric values.
	Values map[string]float64
}

// CH is the interface for fetching one page of metrics from CloudHealth.
// Most clients will not need to use this interface.
type CH interface {
	Fetch(url string) (entries []*Entry, next string, err error)
}

var (
	// DefaultCH is the default implementation of CH.
	DefaultCH CH = &chType{}
)

// Config represents the configuration for a Reader.
type Config struct {
	ApiKey string `yaml:"apiKey"`
}

func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type configFields Config
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*configFields)(c))
}

func (c *Config) Reset() {
	*c = Config{}
}

// Reader is the interface for reading metrics from CloudHealth.
type Reader interface {
	// Read reads the metrics for a particular asset between start time
	// inclusive and end time exclusive. assetId looks like
	// "arn:aws:ec2:us-east-1:12345678901:instance/i-12345678"
	Read(assetId string, start, end time.Time) ([]*Entry, error)
}

// NewMemoizedReader returns a memoized version of r.
func NewMemoizedReader(r Reader) Reader {
	return newMemoizedReader(r)
}

// NewReader creates a new reader
func NewReader(c Config) Reader {
	return &chReaderType{
		config: c,
		ch:     DefaultCH,
		now:    time.Now,
	}
}

// NewCustomReader creates a new reader that uses a custom implementation
// of CH and a custom clock. now is the function returning the current time.
// Clients pass time.Now for the system clock.
func NewCustomReader(c Config, ch CH, now func() time.Time) Reader {
	return &chReaderType{
		config: c,
		ch:     ch,
		now:    now}
}
