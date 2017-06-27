package chreader

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

var (
	kErrMissingMetaData     = errors.New("Metadata chunk missing")
	kErrMissingTimestamp    = errors.New("Missing timestamp")
	kErrWrongNumberOfValues = errors.New("Wrong number of values")
)

type metaDataType struct {
	AssetType   string   `json:"assetType"`
	Granularity string   `json:"granularity"`
	Keys        []string `json:"keys"`
}

type datasetType struct {
	Metadata *metaDataType   `json:"metadata"`
	Values   [][]interface{} `json:"values"`
}

type requestType struct {
	Next *string `json:"next"`
}

type responseType struct {
	Datasets []datasetType `json:"datasets"`
	Request  requestType   `json:"request"`
}

type chType struct {
}

func (c *chType) Fetch(url string) ([]*Entry, string, error) {
	return fetch(url)
}

func fetch(url string) ([]*Entry, string, error) {
	var client http.Client
	resp, err := client.Get(url)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	// If status code is 400 or greater, assume output is the error
	if resp.StatusCode >= 400 {
		var buffer bytes.Buffer
		buffer.ReadFrom(resp.Body)
		return nil, "", errors.New(buffer.String())
	}
	// Otherwise unmarshal response and extract the metric values
	res, err := extractResponse(resp.Body)
	if err != nil {
		return nil, "", err
	}
	return extractMetrics(res)
}

func extractResponse(reader io.Reader) (*responseType, error) {
	decoder := json.NewDecoder(reader)
	var result *responseType
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func extractMetrics(res *responseType) ([]*Entry, string, error) {
	var result []*Entry
	if len(res.Datasets) > 1 {
		return nil, "", errors.New("Multiple datasets not supported")
	}
	for _, dataset := range res.Datasets {
		if dataset.Metadata == nil {
			return nil, "", kErrMissingMetaData
		}
		if err := extractInstanceMetrics(
			dataset.Metadata.Keys, dataset.Values, &result); err != nil {
			return nil, "", err
		}
	}
	nextUrl := ""
	if res.Request.Next != nil {
		nextUrl = *res.Request.Next
	}
	return result, nextUrl, nil
}

func extractInstanceMetrics(
	keys []string, values [][]interface{}, sink *[]*Entry) error {
	for _, group := range values {
		if len(group) != len(keys) {
			return kErrWrongNumberOfValues
		}
		entry := Entry{Values: make(map[string]float64)}
		timeSet := false
		for i, value := range group {
			if keys[i] == "assetId" {
				// do nothing
			} else if keys[i] == "timestamp" {
				timestampStr, ok := value.(string)
				if !ok {
					return fmt.Errorf("%v should be a string.", value)
				}
				timestamp, err := time.Parse(
					"2006-01-02T15:04:05Z07:00", timestampStr)
				if err != nil {
					return err
				}
				entry.Time = timestamp
				timeSet = true
			} else if value != nil {
				val, ok := value.(float64)
				if !ok {
					return fmt.Errorf("%v should be a float.", value)
				}
				entry.Values[keys[i]] = val
			}
		}
		if !timeSet {
			return kErrMissingTimestamp
		}
		*sink = append(*sink, &entry)
	}
	return nil
}
