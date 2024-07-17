package main

import (
	"encoding/hex"
	"sync"

	"git.tcp.direct/kayos/common/hash"
)

type exifTags map[string][]any

func newExifTags() exifTags {
	return make(exifTags)
}

type cachedExifTags struct {
	m  map[string]exifTags
	mu sync.RWMutex
}

func newExifCache() *cachedExifTags {
	return &cachedExifTags{
		m: make(map[string]exifTags),
	}
}

func (ec *cachedExifTags) put(rawExif []byte, tags exifTags) {
	ec.mu.Lock()
	checksum := hex.EncodeToString(hash.Sum(hash.TypeCRC64ECMA, rawExif))
	ec.m[checksum] = tags
	ec.mu.Unlock()
}

func (ec *cachedExifTags) get(rawExif []byte) (exifTags, bool) {
	ec.mu.RLock()
	checksum := hex.EncodeToString(hash.Sum(hash.TypeCRC64ECMA, rawExif))
	tags, ok := ec.m[checksum]
	ec.mu.RUnlock()
	return tags, ok
}

var exifCache = newExifCache()
