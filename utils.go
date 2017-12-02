package ergoq

import (
	"crypto/sha1"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
)

// Returns topic name from dsn. dsn must start with name followed by "://"
func getNameFromDSN(dsn string) (string, error) {
	i := strings.Index(strings.TrimSpace(dsn), "://")
	if i == -1 {
		return "", fmt.Errorf("cannot get name from %s correct format is name://", dsn)
	}
	name := strings.ToLower(dsn[:i])
	if name == "" {
		return "", fmt.Errorf("cannot get name from %s correct format is name://", dsn)
	}
	i = strings.Index(name, "+")
	if i+1 < len(dsn) && i != -1 {
		name = name[:i]
	}
	return name, nil
}

// returns int from url.values if not found return default value
func GetInt(values url.Values, key string, def int) int {

	val := values.Get(key)
	if val == "" {
		return def
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return def
	}

	return i
}

// returns int from url.values if not found return default value
func GetBool(values url.Values, key string, def bool) bool {

	val := values.Get(key)
	if val == "" {
		return def
	}

	l := strings.TrimSpace(strings.ToLower(val))

	trueVals := []string{
		"true", "1", "t",
	}
	for _, tv := range trueVals {
		if l == tv {
			return true
		}
	}

	return false
}

// returns int from url.values if not found return default value
func GetString(values url.Values, key string, def string) string {

	val := values.Get(key)
	if val == "" {
		return def
	}

	l := strings.TrimSpace(strings.ToLower(val))

	if l == "" {
		return def
	}
	return l
}

var (
	// autoincrement holds autoincrementor
	autoincrement = int64(0)
)

// NewID generates autoincrement id (unique)
func NewID() int64 {
	return atomic.AddInt64(&autoincrement, 1)
}

// NewHash generates new random hash
func NewHash() string {
	h := sha1.New()
	io.WriteString(h, strconv.Itoa(int(NewID())))
	result := fmt.Sprintf("%x", h.Sum(nil))[:16]

	final := make([]string, 4)
	for i := 0; i < 4; i++ {
		final[i] = result[i*4:i*4+4]
	}

	return strings.Join(final, "-")
}
