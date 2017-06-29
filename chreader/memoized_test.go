package chreader_test

import (
	"errors"
	"github.com/Symantec/uhura/chreader"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

// fakeReaderType returns entries with timestamps every hour on the hour.
// The metric name in each entry matches the assetId. The metric value matches
// the timestamp as seconds since epoch.
// AssetId of "error" causes error to return
type fakeReaderType struct {
	UseCount int
}

func (r *fakeReaderType) Read(
	assetId string, start, end time.Time) ([]*chreader.Entry, error) {
	r.UseCount++
	if assetId == "error" {
		return nil, errors.New("Got assetId of error")
	}
	// round start up to nearest hour
	newStart := start.Truncate(time.Hour)
	if newStart.Before(start) {
		start = newStart.Add(time.Hour)
	}
	var result []*chreader.Entry
	for ts := start; ts.Before(end); ts = ts.Add(time.Hour) {
		unix := ts.Unix()
		result = append(
			result,
			&chreader.Entry{
				Time: ts,
				Values: map[string]float64{
					assetId: float64(unix),
				},
			})
	}
	return result, nil
}

func TestMemoizedReader(t *testing.T) {

	Convey("With fake reader", t, func() {
		fakeReader := &fakeReaderType{}
		memoizedReader := chreader.NewMemoizedReader(fakeReader)
		Convey("Cache impervious to mutations", func() {
			entries, _ := memoizedReader.Read(
				"instance", kNow.Add(-3*time.Hour), kNow)
			So(entries, ShouldHaveLength, 3)
			entries[0] = nil
			entriesAgain, _ := memoizedReader.Read(
				"instance", kNow.Add(-3*time.Hour), kNow)
			So(entriesAgain, ShouldHaveLength, 3)
			So(entriesAgain[0], ShouldNotBeNil)
		})
		Convey("On hit, no call to underlying reader", func() {
			entries, _ := memoizedReader.Read(
				"instance", kNow.Add(-3*time.Hour), kNow)
			So(entries, ShouldHaveLength, 3)
			entriesAgain, _ := memoizedReader.Read(
				"instance", kNow.Add(-3*time.Hour), kNow)
			So(entries, ShouldResemble, entriesAgain)
			// One call to underlying reader, not 2
			So(fakeReader.UseCount, ShouldEqual, 1)
		})
		Convey("Memoized reader should memoize correctly.", func() {
			entries, _ := memoizedReader.Read(
				"instance", kNow.Add(-3*time.Hour), kNow)
			So(entries, ShouldHaveLength, 3)
			entries1, _ := memoizedReader.Read(
				"different_instance", kNow.Add(-3*time.Hour), kNow)
			So(entries1, ShouldHaveLength, 3)
			entries2, _ := memoizedReader.Read(
				"instance", kNow.Add(-2*time.Hour), kNow)
			So(entries2, ShouldHaveLength, 2)
			entries3, _ := memoizedReader.Read(
				"instance", kNow.Add(-3*time.Hour), kNow.Add(time.Hour))
			So(entries3, ShouldHaveLength, 4)
			So(entries, ShouldNotResemble, entries1)
			So(fakeReader.UseCount, ShouldEqual, 4)
		})
		Convey("Errors should propogate but not be memoized", func() {
			_, err := memoizedReader.Read("error", kNow.Add(-time.Hour), kNow)
			So(err, ShouldNotBeNil)
			_, err1 := memoizedReader.Read("error", kNow.Add(-time.Hour), kNow)
			So(err1, ShouldNotBeNil)
			// 2 calls to underlying reader since errors don't get
			// memoized
			So(fakeReader.UseCount, ShouldEqual, 2)
		})
	})
}
