package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/lib/logbuf"
	"github.com/Symantec/scotty/lib/apiutil"
	"github.com/Symantec/scotty/lib/dynconfig"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/Symantec/tricorder/go/healthserver"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/Symantec/uhura/chreader"
	"github.com/Symantec/uhura/cmd/uhura/splash"
	"github.com/Symantec/uhura/tsdbadapter"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"net/url"
	"path"
	"time"
)

var (
	fPort      = flag.Int("port", 4242, "port")
	fConfigDir = flag.String("configDir", "/etc/uhura", "config Directory")
)

var (
	kTriResponseTimesMillisBucketer = tricorder.NewGeometricBucketer(1e-6, 1e6)
	kTriQueryTimeDist               = kTriResponseTimesMillisBucketer.NewCumulativeDistribution()
)

func registerMetrics() error {
	if err := tricorder.RegisterMetric(
		"/queries/responseTimes",
		kTriQueryTimeDist,
		units.Millisecond,
		"Successful query response times"); err != nil {
		return err
	}
	return nil
}

func main() {
	tricorder.RegisterFlags()
	if err := registerMetrics(); err != nil {
		log.Fatal(err)
	}
	flag.Parse()
	rpc.HandleHTTP()
	circularBuffer := logbuf.New()
	logger := log.New(circularBuffer, "", log.LstdFlags)
	readerConfig, err := dynconfig.NewInitialized(
		path.Join(*fConfigDir, "uhura.yaml"),
		newReader,
		"reader",
		logger)
	if err != nil {
		log.Fatal(err)
	}
	http.Handle("/",
		&splash.Handler{
			Log: circularBuffer,
		})
	http.Handle(
		"/api/query",
		newTsdbHandler(
			func(r *tsdbjson.QueryRequest) ([]tsdbjson.TimeSeries, error) {
				beginTime := time.Now()
				start := r.StartInMillis
				end := r.EndInMillis
				if end == 0 {
					end = time.Now().Unix() * 1000
				}
				reader := chreader.NewMemoizedReader(
					readerConfig.Get().(chreader.Reader))
				var result []tsdbjson.TimeSeries
				for _, query := range r.Queries {
					info, err := extractInfo(query)
					if err != nil {
						return nil, err
					}
					timeSeries, err := fetchTimeSeries(
						reader,
						&info.Asset,
						info.Name,
						start,
						end)
					if err != nil {
						return nil, err
					}
					result = append(result, timeSeries)
				}
				kTriQueryTimeDist.Add(time.Since(beginTime))
				return result, nil
			}))
	http.Handle(
		"/api/suggest",
		newTsdbHandler(
			func(req url.Values) ([]string, error) {
				return []string{}, nil
			}))
	http.Handle(
		"/api/aggregators",
		newTsdbHandler(
			func(req url.Values) ([]string, error) {
				return []string{"avg"}, nil
			}))
	http.Handle(
		"/api/version",
		newTsdbHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"version": "1.0",
				}, nil
			}))
	http.Handle(
		"/api/config",
		newTsdbHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"tsd.ore.auto_create_metrics": "true",
					"tsd.ore.auto_create_tagks":   "true",
					"tsd.ore.auto_create_tagvs":   "true",
				}, nil
			}))
	http.Handle(
		"/api/config/filters",
		newTsdbHandler(
			func(req url.Values) (interface{}, error) {
				return tsdbjson.AllFilterDescriptions(), nil
			}))
	http.Handle(
		"/api/dropcaches",
		newTsdbHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"message": "Caches dropped",
					"status":  "200",
				}, nil
			}))
	http.Handle(
		"/api/",
		newTsdbHandler(
			func(params url.Values) (interface{}, error) {
				return nil, tsdbjson.NewError(
					404, errors.New("Endpoint not found"))
			}))
	healthserver.SetReady()
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *fPort), nil); err != nil {
		log.Fatal(err)
	}
}

var (
	kOptions = &apiutil.Options{
		ErrorGenerator: func(status int, err error) interface{} {
			return tsdbjson.NewError(status, err)
		},
	}
)

func newTsdbHandler(handler interface{}) http.Handler {
	return apiutil.NewHandler(handler, kOptions)
}

type infoType struct {
	Asset tsdbadapter.Asset
	Name  string
}

var (
	kTagsRequired = errors.New("region, accountNumber, and instanceId tags required")
)

var (
	kRegion        = "region"
	kAccountNumber = "accountNumber"
	kInstanceId    = "instanceId"
)

func extractInfo(query *tsdbjson.Query) (*infoType, error) {
	var result infoType
	result.Name = tsdbjson.Unescape(query.Metric)
	for _, filter := range query.Filters {
		switch filter.Tagk {
		case kRegion:
			result.Asset.Region = filter.Filter
		case kAccountNumber:
			result.Asset.AccountNumber = filter.Filter
		case kInstanceId:
			result.Asset.InstanceId = filter.Filter
		}
	}
	for k, v := range query.Tags {
		switch k {
		case kRegion:
			result.Asset.Region = v
		case kAccountNumber:
			result.Asset.AccountNumber = v
		case kInstanceId:
			result.Asset.InstanceId = v
		}
	}
	if result.Asset.Region == "" || result.Asset.AccountNumber == "" || result.Asset.InstanceId == "" {
		return nil, kTagsRequired
	}
	return &result, nil
}

func fetchTimeSeries(
	reader chreader.Reader,
	asset *tsdbadapter.Asset,
	name string,
	start,
	end int64) (tsdbjson.TimeSeries, error) {
	dps, err := tsdbadapter.Fetch(
		reader,
		asset,
		name,
		start,
		end)
	if err != nil {
		return tsdbjson.TimeSeries{}, err
	}
	return tsdbjson.TimeSeries{
		Metric: name,
		Tags: map[string]string{
			kRegion:        asset.Region,
			kAccountNumber: asset.AccountNumber,
			kInstanceId:    asset.InstanceId,
		},
		AggregateTags: []string{},
		Dps:           dps,
	}, nil
}

func newReader(reader io.Reader) (interface{}, error) {
	var config chreader.Config
	if err := yamlutil.Read(reader, &config); err != nil {
		return nil, err
	}
	return chreader.NewReader(config), nil
}
