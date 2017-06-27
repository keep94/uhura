package chreader_test

import (
	"fmt"
	"github.com/Symantec/scotty/lib/httputil"
	"github.com/Symantec/uhura/chreader"
	. "github.com/smartystreets/goconvey/convey"
	"net/url"
	"strconv"
	"testing"
	"time"
)

const (
	kEntriesPerPage = 100
	kAssetId        = "assetId"
	kApiKey         = "apiKey"
)

var (
	kTimeRangeValues = map[string]time.Duration{
		"yesterday":    -24 * time.Hour,
		"last_2_days":  -2 * 24 * time.Hour,
		"last_7_days":  -7 * 24 * time.Hour,
		"last_14_days": -14 * 24 * time.Hour,
		"last_31_days": -31 * 24 * time.Hour,
	}
	kMidnight = time.Date(2017, 6, 20, 0, 0, 0, 0, time.UTC)
	kNow      = time.Date(2017, 6, 26, 12, 0, 0, 0, time.UTC)
)

// fake CH implementation. Returns entries every hour on the hour.
// Returned entries have a time but no data.
type fakeCHType struct {
	// The current time.
	CurrentTime time.Time

	// expected API key
	ApiKey string

	// expected asset Id
	AssetId string
}

func (ch *fakeCHType) Fetch(rawUrl string) ([]*chreader.Entry, string, error) {
	url, err := url.Parse(rawUrl)
	if err != nil {
		return nil, "", err
	}
	values := url.Query()
	apiKey := values.Get("api_key")
	assetId := values.Get("asset")
	timeRange := values.Get("time_range")
	pageStr := values.Get("page")
	page, _ := strconv.Atoi(pageStr)
	// default page is 1 no page 0
	if page == 0 {
		page = 1
	}
	if apiKey != ch.ApiKey {
		return nil, "", fmt.Errorf(
			"Expected API key '%s', got '%s'", ch.ApiKey, apiKey)
	}
	if assetId != ch.AssetId {
		return nil, "", fmt.Errorf(
			"Expected asset '%s', got '%s'", ch.AssetId, assetId)
	}
	inUTC := ch.CurrentTime.UTC()
	midnight := time.Date(inUTC.Year(), inUTC.Month(), inUTC.Day(), 0, 0, 0, 0, time.UTC)
	var entries []*chreader.Entry
	var nextPage int
	if timeRange == "today" {
		entries, nextPage = entriesByPage(
			entriesFromTo(midnight, ch.CurrentTime),
			page)
	} else {
		timeAgo, ok := kTimeRangeValues[timeRange]
		if !ok {
			return nil, "", fmt.Errorf("time range '%s' not recognised.", timeRange)
		}
		entries, nextPage = entriesByPage(
			entriesFromTo(midnight.Add(timeAgo), midnight),
			page)
	}
	if nextPage != 0 {
		newUrl := httputil.WithParams(url, "page", strconv.Itoa(nextPage))
		return entries, newUrl.String(), nil
	}
	return entries, "", nil
}

func entriesFromTo(start, end time.Time) (entries []*chreader.Entry) {
	currentTime := start.Truncate(time.Hour)
	if currentTime.Before(start) {
		currentTime = currentTime.Add(time.Hour)
	}
	for currentTime.Before(end) {
		entries = append(entries, &chreader.Entry{Time: currentTime})
		currentTime = currentTime.Add(time.Hour)
	}
	return
}

func entriesByPage(entries []*chreader.Entry, page int) (
	result []*chreader.Entry, nextPage int) {
	start := (page - 1) * kEntriesPerPage
	end := page * kEntriesPerPage
	entryLen := len(entries)
	if end > entryLen {
		end = entryLen
	}
	for i := start; i < end; i++ {
		result = append(result, entries[i])
	}
	if end < entryLen {
		nextPage = page + 1
	}
	return
}

func TestReader(t *testing.T) {

	Convey("With fake cloudhealth", t, func() {
		fakeCh := &fakeCHType{
			CurrentTime: kNow,
			ApiKey:      kApiKey,
			AssetId:     kAssetId}
		Convey("With valid reader", func() {
			reader := chreader.NewCustomReader(
				chreader.Config{
					ApiKey: kApiKey,
				},
				fakeCh,
				func() time.Time {
					return kNow
				},
			)
			Convey("Future range returns no entries", func() {
				entries, err := reader.Read(
					kAssetId, kNow, kNow.Add(20*time.Hour))
				So(entries, ShouldBeEmpty)
				So(err, ShouldBeNil)
			})
			Convey("Today range", func() {
				entries, err := reader.Read(
					kAssetId, kMidnight, kMidnight.Add(3*time.Hour))
				So(
					entries,
					shouldHaveRange,
					kMidnight,
					kMidnight.Add(3*time.Hour))
				So(err, ShouldBeNil)
			})
			Convey("Yesterday and today range", func() {
				entries, err := reader.Read(
					kAssetId, kMidnight.Add(-270*time.Minute), kNow)
				So(
					entries,
					shouldHaveRange,
					kMidnight.Add(-4*time.Hour),
					kNow)
				So(err, ShouldBeNil)
			})
			Convey("Before yesterday", func() {
				entries, err := reader.Read(
					kAssetId,
					kMidnight.Add(-25*time.Hour),
					kMidnight.Add(-20*time.Hour))
				So(
					entries,
					shouldHaveRange,
					kMidnight.Add(-25*time.Hour),
					kMidnight.Add(-20*time.Hour))
				So(err, ShouldBeNil)
			})
			Convey("7 days ago", func() {
				entries, err := reader.Read(
					kAssetId,
					kMidnight.Add(-160*time.Hour),
					kMidnight.Add(-20*time.Hour))
				So(
					entries,
					shouldHaveRange,
					kMidnight.Add(-160*time.Hour),
					kMidnight.Add(-20*time.Hour))
				So(err, ShouldBeNil)
			})
			Convey("14 days ago", func() {
				entries, err := reader.Read(
					kAssetId,
					kMidnight.Add(-169*time.Hour),
					kMidnight.Add(-167*time.Hour))
				So(
					entries,
					shouldHaveRange,
					kMidnight.Add(-169*time.Hour),
					kMidnight.Add(-167*time.Hour))
				So(err, ShouldBeNil)
			})
			Convey("31 days ago and today", func() {
				entries, err := reader.Read(
					kAssetId,
					kMidnight.Add(-400*time.Hour),
					kMidnight.Add(3*time.Hour))
				So(
					entries,
					shouldHaveRange,
					kMidnight.Add(-400*time.Hour),
					kMidnight.Add(3*time.Hour))
				So(err, ShouldBeNil)
			})
			Convey("Way in the past", func() {
				entries, err := reader.Read(
					kAssetId,
					kMidnight.Add(-2000*time.Hour),
					kMidnight.Add(-1000*time.Hour))
				So(entries, ShouldBeEmpty)
				So(err, ShouldBeNil)
			})
		})
		Convey("With wrong API Key", func() {
			reader := chreader.NewCustomReader(
				chreader.Config{
					ApiKey: "wrongKey",
				},
				fakeCh,
				func() time.Time {
					return kNow
				},
			)
			Convey("Wrong API Key should throw error", func() {
				_, err := reader.Read(
					kAssetId,
					kMidnight.Add(-20*time.Hour),
					kMidnight)
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestTimeSkew(t *testing.T) {
	// TODO
}

func shouldHaveRange(actual interface{}, expected ...interface{}) string {
	entries := actual.([]*chreader.Entry)
	start := expected[0].(time.Time)
	end := expected[1].(time.Time)
	idx := 0
	for ctime := start; ctime.Before(end); ctime = ctime.Add(time.Hour) {
		if idx == len(entries) {
			return fmt.Sprintf("Ran out of entries at [%d]", idx)
		}
		if entries[idx].Time != ctime {
			return fmt.Sprintf(
				"At [%d], expected %v, got %v", ctime, entries[idx].Time)
		}
		idx++
	}
	if idx != len(entries) {
		return fmt.Sprintf("Expected %d entries, got %d", idx, len(entries))
	}
	return ""
}
