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
	kNow      = time.Date(2017, 6, 20, 12, 0, 0, 0, time.UTC)
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

	// number of times Fetch called
	CallCount int
}

func (ch *fakeCHType) Fetch(rawUrl string) ([]*chreader.Entry, string, error) {
	ch.CallCount++
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
				So(fakeCh.CallCount, ShouldEqual, 1)
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
				So(fakeCh.CallCount, ShouldEqual, 1)
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
				So(fakeCh.CallCount, ShouldEqual, 2)
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
				So(fakeCh.CallCount, ShouldEqual, 1)
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

				// 100 hour batches
				So(fakeCh.CallCount, ShouldEqual, 2)
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
				So(fakeCh.CallCount, ShouldEqual, 2)
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

				// 31*24=744 =8 batches + today
				So(fakeCh.CallCount, ShouldEqual, 9)
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
	Convey("cloudhealth server is one day earlier. Calling 'today' on cloud health server gives yesterday's data", t, func() {
		fakeCh := &fakeCHType{
			CurrentTime: kMidnight.Add(-1 * time.Minute),
			ApiKey:      kApiKey,
			AssetId:     kAssetId}
		reader := chreader.NewCustomReader(
			chreader.Config{
				ApiKey: kApiKey,
			},
			fakeCh,
			func() time.Time {
				return kNow
			},
		)
		Convey("Get yesterday's data", func() {
			entries, err := reader.Read(
				kAssetId, kMidnight.Add(-270*time.Minute), kMidnight)
			So(
				entries,
				shouldHaveRange,
				kMidnight.Add(-4*time.Hour),
				kMidnight)
			So(err, ShouldBeNil)
			// We end up calling "yesterday" and "today" on cloudhealth server
			// just to get yesterday's data
			So(fakeCh.CallCount, ShouldEqual, 2)
		})
		Convey("Get data from 2 days ago", func() {
			entries, err := reader.Read(
				kAssetId, kMidnight.Add(-48*time.Hour), kMidnight.Add(-25*time.Hour))
			So(
				entries,
				shouldHaveRange,
				kMidnight.Add(-48*time.Hour),
				kMidnight.Add(-25*time.Hour))
			So(err, ShouldBeNil)
			// We call "last_2_days" on cloud health but get back
			// 3 days ago to yesterday which has everything we need.
			So(fakeCh.CallCount, ShouldEqual, 1)
		})
		Convey("Get data from 2 days ago up to 1 day ago", func() {
			entries, err := reader.Read(
				kAssetId, kMidnight.Add(-48*time.Hour), kMidnight.Add(-24*time.Hour))
			So(
				entries,
				shouldHaveRange,
				kMidnight.Add(-48*time.Hour),
				kMidnight.Add(-24*time.Hour))
			So(err, ShouldBeNil)
			// We call "last_2_days" on cloud health but get back
			// 3 days ago to yesterday, but since we don't see anything
			// after our end time, we also get "today" data which is
			// really yesterday's data.
			So(fakeCh.CallCount, ShouldEqual, 2)
		})
	})
	Convey("cloudhealth server one day after. Calling yesterday on cloudhealth gives today's data", t, func() {
		fakeCh := &fakeCHType{
			CurrentTime: kMidnight.Add(24 * time.Hour).Add(time.Minute),
			ApiKey:      kApiKey,
			AssetId:     kAssetId}
		reader := chreader.NewCustomReader(
			chreader.Config{
				ApiKey: kApiKey,
			},
			fakeCh,
			func() time.Time {
				return kNow
			},
		)
		Convey("start on what we think is today", func() {
			entries, err := reader.Read(
				kAssetId, kMidnight, kMidnight.Add(3*time.Hour))
			So(
				entries,
				shouldHaveRange,
				kMidnight,
				kMidnight.Add(3*time.Hour))
			So(err, ShouldBeNil)
			// We try calling "today" on cloudhealth but find we have to
			// call "yesterday" to get today's data
			So(fakeCh.CallCount, ShouldEqual, 2)
		})
		Convey("Get data from 2 days go though midnight today", func() {
			entries, err := reader.Read(
				kAssetId, kMidnight.Add(-48*time.Hour), kMidnight)
			So(
				entries,
				shouldHaveRange,
				kMidnight.Add(-48*time.Hour),
				kMidnight)
			So(err, ShouldBeNil)
			// We try calling last_2_days cloudhealth but we just get
			// yesterdays data. So we have to call last_7_days which
			// gives us 6 days ago until tomorrow
			// 6 days = 144 hours = 2 batches
			// 3 = 2 batches + original "last_2_days" call
			So(fakeCh.CallCount, ShouldEqual, 3)
		})
	})
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
