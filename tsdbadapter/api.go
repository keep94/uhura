// Package tsdbadapter reads a single metric for openTSDB.
package tsdbadapter

import (
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/uhura/chreader"
)

// An Asset represents a specific machine in an AWS fleet
type Asset struct {
	Region        string // like "us-east-1"
	AccountNumber string // owner account number like "12345678901"
	InstanceId    string // like "i-12345678"
}

// Fetch fetches a time series for a single metric for openTSDB.
// reader reads metrics from CloudHealth.
// asset identifies the machine in AWS
// name is the name of the metric
// start is the start time in milliseconds since epoch inclusive
// end is the end time in milliseconds since epoch exclusive.
func Fetch(
	reader chreader.Reader,
	asset *Asset,
	name string,
	start,
	end int64) (tsdb.TimeSeries, error) {
	return fetch(
		reader,
		asset,
		name,
		start,
		end)
}
