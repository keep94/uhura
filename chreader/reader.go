package chreader

import (
	"github.com/Symantec/scotty/lib/httputil"
	"net/url"
	"sort"
	"time"
)

var (
	kCHUrl = mustParseUrl("https://chapi.cloudhealthtech.com/metrics/v1")
)

type chReaderType struct {
	config Config
	ch     CH
	now    func() time.Time
}

func (r *chReaderType) Read(assetId string, start, end time.Time) (
	[]*Entry, error) {
	now := r.now().UTC()
	start = start.UTC()
	end = end.UTC()
	midnight := time.Date(
		now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	var entries []*Entry
	if start.Before(midnight) {
		timeRange := computeTimeRange(midnight.Sub(start))
		var pastEntries []*Entry
		var err error
		if end.After(midnight) {
			pastEntries, err = r.getEntries(assetId, timeRange, start, midnight)
		} else {
			pastEntries, err = r.getEntries(assetId, timeRange, start, end)
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, pastEntries...)
	}
	if end.After(midnight) {
		var todaysEntries []*Entry
		var err error
		if start.Before(midnight) {
			todaysEntries, err = r.getEntries(assetId, "today", midnight, end)
		} else {
			todaysEntries, err = r.getEntries(assetId, "today", start, end)
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, todaysEntries...)
	}
	return entries, nil
}

func (r *chReaderType) getEntries(
	assetId,
	timeRange string,
	start,
	end time.Time) ([]*Entry, error) {
	var result []*Entry
	nextUrl, err := r.appendBatch(
		r.computeUrlStr(assetId, timeRange), start, end, &result)
	if err != nil {
		return nil, err
	}
	for nextUrl != "" {
		nextUrl, err = r.appendBatch(nextUrl, start, end, &result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (r *chReaderType) appendBatch(
	url string, start, end time.Time, sink *[]*Entry) (string, error) {
	entries, nextUrl, err := r.ch.Fetch(url)
	if err != nil {
		return "", err
	}
	elen := len(entries)
	startIdx := 0
	// Find first entry coming on or after start. We don't use binary search
	// here in case cloudhealth gives us data in unsorted order.
	for startIdx < elen && entries[startIdx].Time.Before(start) {
		startIdx++
	}
	endIdx := startIdx
	// Find first entry coming on or after end.
	for endIdx < elen && entries[endIdx].Time.Before(end) {
		endIdx++
	}

	// add entries to sink
	*sink = append(*sink, entries[startIdx:endIdx]...)

	// This batch goes past end. Return empty next url to signal we are done.
	if endIdx < elen {
		return "", nil
	}
	return nextUrl, nil
}

func (r *chReaderType) computeUrlStr(assetId, timeRange string) string {
	return httputil.AppendParams(
		kCHUrl,
		"api_key", r.config.ApiKey,
		"asset", assetId,
		"time_range", timeRange).String()
}

type timeRangeType struct {
	Dur  time.Duration
	Name string
}

var (
	kTimeRanges = []timeRangeType{
		{Dur: 24 * time.Hour, Name: "yesterday"},
		{Dur: 2 * 24 * time.Hour, Name: "last_2_days"},
		{Dur: 7 * 24 * time.Hour, Name: "last_7_days"},
		{Dur: 14 * 24 * time.Hour, Name: "last_14_days"},
		{Dur: 31 * 24 * time.Hour, Name: "last_31_days"},
	}
)

func computeTimeRange(dur time.Duration) string {
	rangeLen := len(kTimeRanges)
	idx := sort.Search(
		rangeLen,
		func(i int) bool { return kTimeRanges[i].Dur >= dur })
	if idx == rangeLen {
		return kTimeRanges[rangeLen-1].Name
	}
	return kTimeRanges[idx].Name
}

func mustParseUrl(urlStr string) *url.URL {
	result, err := url.Parse(urlStr)
	if err != nil {
		panic(err)
	}
	return result
}
