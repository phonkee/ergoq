package ergoq

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Returns queue name from dsn. dsn must start with name followed by "://"
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
