package chreader

import (
	"time"
)

type memoizedReaderKeyType struct {
	AssetId string
	Start   time.Time
	End     time.Time
}

type memoizedReaderType struct {
	r    Reader
	data map[memoizedReaderKeyType][]*Entry
}

func (c *memoizedReaderType) Read(assetId string, start, end time.Time) (
	[]*Entry, error) {
	// Convert to UTC to ensure cache keys match
	start = start.UTC()
	end = end.UTC()
	key := memoizedReaderKeyType{
		AssetId: assetId,
		Start:   start,
		End:     end}
	result, ok := c.data[key]
	if !ok {
		var err error
		result, err = c.r.Read(assetId, start, end)
		if err != nil {
			return nil, err
		}
		c.data[key] = result
	}
	// Return defensive copy to protect cache
	resultCopy := make([]*Entry, len(result))
	copy(resultCopy, result)
	return resultCopy, nil
}

func newMemoizedReader(r Reader) Reader {
	return &memoizedReaderType{
		r: r, data: make(map[memoizedReaderKeyType][]*Entry)}
}
