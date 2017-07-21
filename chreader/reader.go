package chreader

import (
	"errors"
	"github.com/Symantec/scotty/lib/httputil"
	"net/url"
	"sort"
	"time"
)

var (
	kCHUrl = mustParseUrl("https://chapi.cloudhealthtech.com/metrics/v1")
)

var (
	kErrDayChanged = errors.New("chreader: Day changed.")
)

type chReaderType struct {
	config Config
	ch     CH
	now    func() time.Time
}

func (r *chReaderType) Read(assetId string, start, end time.Time) (
	[]*Entry, error) {
	entries, err := r.read(assetId, start, end)

	// If current day changed on the cloud health servers during our query,
	// just start over.
	for err == kErrDayChanged {
		entries, err = r.read(assetId, start, end)
	}
	return entries, err
}

func (r *chReaderType) read(assetId string, start, end time.Time) (
	[]*Entry, error) {
	now := r.now().UTC()
	start = start.UTC()
	end = end.UTC()
	midnight := time.Date(
		now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	var entries []*Entry

	// Here we keep the Time in the Date content header of the last response
	// from cloudhealth. We use this to raise kErrDayChanged if the current
	// day on the cloud health servers changes in the middle of our work.
	var lastBatchTime time.Time // zero value means no value

	// If start is before midnight, use 'yesterday' or 'last_2_days' etc.
	if start.Before(midnight) {

		// compute what time range to use e.g "last_2_days"
		timeRangeIdx := computeTimeRangeIdx(midnight.Sub(start))

		// Get all the entries but if the first entry comes after the start
		// time we might have clock skew so exit early before fetching all the
		// entries. That is what "true" means.
		pastEntries, earlyEnough, lateEnough, err := r.getEntries(
			assetId,
			currentTimeRange(timeRangeIdx),
			start, end,
			&lastBatchTime,
			true)
		if err != nil {
			return nil, err
		}

		// If we may have clock skew, use the previous time range just to
		// be sure we have everything. e.g "last_7_days" becomes "last_14_days"
		if !earlyEnough {
			pastEntries, _, lateEnough, err = r.getEntries(
				assetId,
				previousTimeRange(timeRangeIdx),
				start,
				end,
				&lastBatchTime,
				false)
			if err != nil {
				return nil, err
			}
		}
		entries = append(entries, pastEntries...)

		// If for whatever reason, clock skew or not, we don't read any
		// entries past the end time, supplement with entries from "today"
		// we do this because our past entries queries only go up to
		// midnight of the current day
		if !lateEnough {
			todaysEntries, _, _, err := r.getEntries(
				assetId, "today", start, end, &lastBatchTime, false)
			if err != nil {
				return nil, err
			}
			entries = append(entries, todaysEntries...)
		}
	} else {
		// start time falls in "today" just get today's entries
		todaysEntries, earlyEnough, _, err := r.getEntries(
			assetId, "today", start, end, &lastBatchTime, true)
		if err != nil {
			return nil, err
		}
		// If we read timestamps on or before start time, we are done
		if earlyEnough {
			return todaysEntries, nil
		}

		// If the earliest entry we read comes after the start time, we may
		// have clock skew. Supplement with yesterday's entries for good
		// measure.
		pastEntries, _, lateEnough, err := r.getEntries(
			assetId, "yesterday", start, end, &lastBatchTime, false)
		if err != nil {
			return nil, err
		}
		entries = append(entries, pastEntries...)

		// If we don't read entries past the end time, re-get today's data
		if !lateEnough {
			todaysEntriesAgain, _, _, err := r.getEntries(
				assetId, "today", start, end, &lastBatchTime, false)
			if err != nil {
				return nil, err
			}
			entries = append(entries, todaysEntriesAgain...)
		}
	}
	return entries, nil
}

func (r *chReaderType) getEntries(
	assetId,
	timeRange string, // "last_2_days", "last_7_days", etc.
	start,
	end time.Time,
	lastBatchTime *time.Time,
	exitEarly bool) (
	result []*Entry, earlyEnough bool, lateEnough bool, err error) {
	var chResult *CHResult
	chResult, err = r.ch.Fetch(r.computeUrlStr(assetId, timeRange))
	if err != nil {
		return
	}
	batchTime, pderr := time.Parse(
		"Mon, 2 Jan 2006 15:04:05 MST", chResult.Date)
	if pderr == nil {
		batchTime = batchTime.UTC()
		if !lastBatchTime.IsZero() && batchTime.Day() != lastBatchTime.Day() {
			err = kErrDayChanged
			*lastBatchTime = batchTime
			return
		}
		*lastBatchTime = batchTime
	}

	batchEntries := chResult.Entries
	nextUrl := chResult.Next

	// See if we have fetched an entry that is on or before the start time
	// If so, set early enough flag
	if len(batchEntries) > 0 && !batchEntries[0].Time.After(start) {
		earlyEnough = true
	} else if exitEarly {
		// if exit early flag set, exit if we know we aren't early enough
		return
	}

	startIdx, endIdx := findRange(batchEntries, start, end)
	result = append(result, batchEntries[startIdx:endIdx]...)

	// See if we read an entry on or after the end time
	// If so, we are done.
	if endIdx < len(batchEntries) {
		lateEnough = true
		return
	}

	// As long as there is a next page
	for nextUrl != "" {
		chResult, err = r.ch.Fetch(nextUrl)
		if err != nil {
			return
		}
		batchEntries = chResult.Entries
		nextUrl = chResult.Next
		startIdx, endIdx := findRange(batchEntries, start, end)
		result = append(result, batchEntries[startIdx:endIdx]...)

		// If we read an entry on or after the end time we are done
		if endIdx < len(batchEntries) {
			lateEnough = true
			return
		}
	}
	return
}

// findRange returns the start and end index to entries that contain only
// times between start inclusive and end exclusive.
func findRange(entries []*Entry, start, end time.Time) (
	startIdx, endIdx int) {
	elen := len(entries)
	startIdx = 0
	// Find first entry coming on or after start. We don't use binary search
	// here in case cloudhealth gives us data in unsorted order.
	for startIdx < elen && entries[startIdx].Time.Before(start) {
		startIdx++
	}
	endIdx = startIdx
	// Find first entry coming on or after end.
	for endIdx < elen && entries[endIdx].Time.Before(end) {
		endIdx++
	}
	return
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

func currentTimeRange(idx int) string {
	return kTimeRanges[idx].Name
}

func previousTimeRange(idx int) string {
	if idx == len(kTimeRanges)-1 {
		return kTimeRanges[idx].Name
	}
	return kTimeRanges[idx+1].Name
}

func computeTimeRangeIdx(dur time.Duration) int {
	rangeLen := len(kTimeRanges)
	idx := sort.Search(
		rangeLen,
		func(i int) bool { return kTimeRanges[i].Dur >= dur })
	if idx == rangeLen {
		return rangeLen - 1
	}
	return idx
}

func mustParseUrl(urlStr string) *url.URL {
	result, err := url.Parse(urlStr)
	if err != nil {
		panic(err)
	}
	return result
}
