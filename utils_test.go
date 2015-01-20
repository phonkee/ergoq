package ergoq

import (
	"net/url"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestName(t *testing.T) {
	Convey("Test getNameFromDSN", t, func() {
		names := []struct {
			dsn   string
			name  string
			isErr bool
		}{
			{"name://", "name", false},
			{"redis://", "redis", false},
			{"redis+something://", "redis", false},
			{"://", "redis", true},
			{"nieco", "", true},
		}

		for _, nm := range names {
			n, e := getNameFromDSN(nm.dsn)
			if nm.isErr {
				So(e, ShouldNotBeNil)
			} else {
				So(e, ShouldBeNil)
				So(n, ShouldEqual, nm.name)
			}
		}
	})

	Convey("Test GetInt", t, func() {
		key := "k"
		data := []struct {
			values   url.Values
			name     string
			expected int
			def      int
		}{
			{url.Values{key: []string{"12"}}, key, 12, 0},
			{url.Values{key: []string{"a"}}, key, 23, 23},
		}

		for _, item := range data {
			ret := GetInt(item.values, item.name, item.def)
			So(ret, ShouldEqual, item.expected)
		}
	})

	Convey("Test GetBool", t, func() {
		key := "k"
		data := []struct {
			values   url.Values
			name     string
			expected bool
			def      bool
		}{
			{url.Values{key: []string{"true"}}, key, true, false},
			{url.Values{key: []string{"t"}}, key, true, false},
			{url.Values{key: []string{"1"}}, key, true, false},
			{url.Values{key: []string{"d"}}, key, false, false},
		}

		for _, item := range data {
			ret := GetBool(item.values, item.name, item.def)
			So(ret, ShouldEqual, item.expected)
		}
	})

	Convey("Test GetString", t, func() {
		key := "k"
		data := []struct {
			values   url.Values
			name     string
			expected string
			def      string
		}{
			{url.Values{key: []string{"yes"}}, key, "yes", "nope"},
			{url.Values{}, key, "nope", "nope"},
		}

		for _, item := range data {
			ret := GetString(item.values, item.name, item.def)
			So(ret, ShouldEqual, item.expected)
		}
	})

}
