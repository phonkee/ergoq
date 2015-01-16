package ergoq

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSubscriberMessage(t *testing.T) {
	test_queue := "queue"
	test_message := []byte("message")

	Convey("test push/pop message", t, func() {
		sm := NewSubscriberMessage(test_queue, test_message)
		So(sm.Message(), ShouldResemble, test_message)
		So(sm.Queue(), ShouldEqual, test_queue)
	})
}
