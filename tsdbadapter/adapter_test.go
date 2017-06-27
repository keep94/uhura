package tsdbadapter_test

import (
	"fmt"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/uhura/chreader"
	"github.com/Symantec/uhura/tsdbadapter"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var (
	kNow       = time.Date(2017, 6, 20, 16, 0, 0, 0, time.UTC)
	kNowMillis = kNow.Unix() * 1000
	kNowSecs   = float64(kNow.Unix())
)

// fakeReaderType returns entries with timestamps every hour on the hour
// Within each entry are 2 metrics: "cpu:even" and "cpu:odd" which contain
// hours since epoch. "cpu:even" is populated on even hours; "cpu:odd" is
// populated on odd hours.
//
// If the assetId passed to Read matches FsAssetId instead of AssetId, the
// metrics are called "fs:even" and "fs:odd"
type fakeReaderType struct {
	// the expected asset id for cpu:even and cpu:odd
	AssetId string
	// the expected asset id for fs:even and fs:odd
	FsAssetId string

	// File system assetId use count
	FsUseCount int
	// instance assetId use count
	InstanceUseCount int
}

func (r *fakeReaderType) Read(
	assetId string, start, end time.Time) ([]*chreader.Entry, error) {
	var evenName, oddName string
	if assetId == r.AssetId {
		evenName = "cpu:even"
		oddName = "cpu:odd"
		r.InstanceUseCount++
	} else if assetId == r.FsAssetId {
		evenName = "fs:even"
		oddName = "fs:odd"
		r.FsUseCount++
	} else {
		return nil, fmt.Errorf("got unrecognised asset Id '%s'", assetId)
	}
	// round start up to nearest hour
	newStart := start.Truncate(time.Hour)
	if newStart.Before(start) {
		start = newStart.Add(time.Hour)
	}
	var result []*chreader.Entry
	for ts := start; ts.Before(end); ts = ts.Add(time.Hour) {
		unix := ts.Unix()
		if unix%7200 == 0 {
			// Even hour
			result = append(
				result,
				&chreader.Entry{
					Time: ts,
					Values: map[string]float64{
						evenName: float64(unix) / 3600.0,
					},
				})
		} else {
			// odd hour
			result = append(
				result,
				&chreader.Entry{
					Time: ts,
					Values: map[string]float64{
						oddName: float64(unix) / 3600.0,
					},
				})
		}
	}
	return result, nil
}

func TestAdapter(t *testing.T) {

	Convey("With fake reader", t, func() {
		fakeReader := &fakeReaderType{
			AssetId:   "arn:aws:ec2:us-east-1:12345:instance/i-12345678",
			FsAssetId: "arn:aws:ec2:us-east-1:12345:instance/i-12345678:fs//",
		}
		asset := tsdbadapter.Asset{
			Region:        "us-east-1",
			AccountNumber: "12345",
			InstanceId:    "i-12345678",
		}
		Convey("cpu:even metric using instance asset", func() {
			timeSeries, err := tsdbadapter.Fetch(
				fakeReader,
				&asset,
				"cpu:even",
				kNowMillis,
				kNowMillis+5*3600*1000)
			So(
				timeSeries,
				ShouldResemble,
				tsdb.TimeSeries{
					{
						Ts:    kNowSecs,
						Value: kNowSecs / 3600.0,
					},
					{
						Ts:    kNowSecs + 2.0*3600.0,
						Value: kNowSecs/3600.0 + 2.0,
					},
					{
						Ts:    kNowSecs + 4.0*3600.0,
						Value: kNowSecs/3600.0 + 4.0,
					},
				})
			So(err, ShouldBeNil)
			So(fakeReader.FsUseCount, ShouldEqual, 0)
			So(fakeReader.InstanceUseCount, ShouldEqual, 1)
		})
		Convey("cpu:odd metric using instance asset", func() {
			timeSeries, err := tsdbadapter.Fetch(
				fakeReader,
				&asset,
				"cpu:odd",
				kNowMillis,
				kNowMillis+5*3600*1000)
			So(
				timeSeries,
				ShouldResemble,
				tsdb.TimeSeries{
					{
						Ts:    kNowSecs + 3600.0,
						Value: kNowSecs/3600.0 + 1.0,
					},
					{
						Ts:    kNowSecs + 3.0*3600.0,
						Value: kNowSecs/3600.0 + 3.0,
					},
				})
			So(err, ShouldBeNil)
			So(fakeReader.FsUseCount, ShouldEqual, 0)
			So(fakeReader.InstanceUseCount, ShouldEqual, 1)
		})
		Convey("cpu:none metric using instance asset", func() {
			timeSeries, err := tsdbadapter.Fetch(
				fakeReader,
				&asset,
				"cpu:none",
				kNowMillis,
				kNowMillis+5*3600*1000)
			So(timeSeries, ShouldBeEmpty)
			So(err, ShouldBeNil)
			So(fakeReader.FsUseCount, ShouldEqual, 0)
			So(fakeReader.InstanceUseCount, ShouldEqual, 1)
		})

		Convey("fs:even metric using fs asset", func() {
			timeSeries, err := tsdbadapter.Fetch(
				fakeReader,
				&asset,
				"fs:even",
				kNowMillis,
				kNowMillis+5*3600*1000)
			So(
				timeSeries,
				ShouldResemble,
				tsdb.TimeSeries{
					{
						Ts:    kNowSecs,
						Value: kNowSecs / 3600.0,
					},
					{
						Ts:    kNowSecs + 2.0*3600.0,
						Value: kNowSecs/3600.0 + 2.0,
					},
					{
						Ts:    kNowSecs + 4.0*3600.0,
						Value: kNowSecs/3600.0 + 4.0,
					},
				})
			So(err, ShouldBeNil)
			So(fakeReader.FsUseCount, ShouldEqual, 1)
			So(fakeReader.InstanceUseCount, ShouldEqual, 0)
		})
		Convey("fs:odd metric using fs asset", func() {
			timeSeries, err := tsdbadapter.Fetch(
				fakeReader,
				&asset,
				"fs:odd",
				kNowMillis,
				kNowMillis+5*3600*1000)
			So(
				timeSeries,
				ShouldResemble,
				tsdb.TimeSeries{
					{
						Ts:    kNowSecs + 3600.0,
						Value: kNowSecs/3600.0 + 1.0,
					},
					{
						Ts:    kNowSecs + 3.0*3600.0,
						Value: kNowSecs/3600.0 + 3.0,
					},
				})
			So(err, ShouldBeNil)
			So(fakeReader.FsUseCount, ShouldEqual, 1)
			So(fakeReader.InstanceUseCount, ShouldEqual, 0)
		})
		Convey("fs:none metric using fs asset", func() {
			timeSeries, err := tsdbadapter.Fetch(
				fakeReader,
				&asset,
				"fs:none",
				kNowMillis,
				kNowMillis+5*3600*1000)
			So(timeSeries, ShouldBeEmpty)
			So(err, ShouldBeNil)
			So(fakeReader.FsUseCount, ShouldEqual, 1)
			So(fakeReader.InstanceUseCount, ShouldEqual, 0)
		})
	})
}
