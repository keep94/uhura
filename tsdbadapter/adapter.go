package tsdbadapter

import (
	"fmt"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/uhura/chreader"
	"strings"
	"time"
)

func fetch(
	reader chreader.Reader,
	asset *Asset,
	name string,
	start,
	end int64) (tsdb.TimeSeries, error) {
	fsMetric := strings.HasPrefix(name, "fs:")
	entries, err := reader.Read(
		computeAssetId(asset, fsMetric),
		millisToTime(start),
		millisToTime(end))
	if err != nil {
		return nil, err
	}
	var result tsdb.TimeSeries
	for _, entry := range entries {
		val, ok := entry.Values[name]
		if ok {
			result = append(
				result,
				tsdb.TsValue{
					Ts:    float64(entry.Time.Unix()),
					Value: val,
				})
		}
	}
	return result, nil
}

func millisToTime(millis int64) time.Time {
	mils := millis % 1000
	secs := millis / 1000
	return time.Unix(secs, mils*1000*1000)
}

func computeAssetId(asset *Asset, fsMetric bool) string {
	if fsMetric {
		return fmt.Sprintf(
			"arn:aws:ec2:%s:%s:instance/%s:fs//",
			asset.Region,
			asset.AccountNumber,
			asset.InstanceId)
	}
	return fmt.Sprintf(
		"arn:aws:ec2:%s:%s:instance/%s",
		asset.Region,
		asset.AccountNumber,
		asset.InstanceId)
}
