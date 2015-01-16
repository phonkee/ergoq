package ergoq

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMessageQueueDriver(t *testing.T) {
	Convey("register driver", t, func() {
		name := "new-redis"

		So(func() { Register(name, nil) }, ShouldPanic)
		So(func() { Register(name, &redisDriver{}) }, ShouldNotPanic)
		So(func() { Register(name, &redisDriver{}) }, ShouldPanic)
		So(Drivers(), ShouldContain, name)
	})

	Convey("test Open", t, func() {
		invalidDsn := "://"
		var err error
		_, err = Open(invalidDsn)
		So(err, ShouldNotBeNil)

		unknownDsn := "pop3://"
		_, err = Open(unknownDsn)
		So(err, ShouldNotBeNil)
	})

	Convey("test OpenConnection", t, func() {
		unknownDriver := "pop3"
		var err error
		_, err = OpenConnection(unknownDriver, nil)
		So(err, ShouldNotBeNil)

	})
}

func TestDrivers(t *testing.T) {
	dsns := []string{
		"redis://localhost:6379",
		"amqp://guest:guest@localhost:5672//test?reliable=true",
	}
	for _, dsn := range dsns {
		mq, err := Open(dsn)

		if err != nil {
			t.Fatalf("Error: %+v\n", err)
		}

		driverName, err := getNameFromDSN(dsn)
		if err != nil {
			t.Error(err)
		}

		Convey(fmt.Sprintf("test push/pop message driver:%s", driverName), t, func() {
			test_queue := "queue"
			test_message := []byte("message")

			errPushBlank := mq.Push(test_queue)
			So(errPushBlank, ShouldNotBeNil)

			errPush := mq.Push(test_queue, test_message)
			So(errPush, ShouldBeNil)

			time.Sleep(2)

			v, errPop := mq.Pop(test_queue)
			So(errPop, ShouldBeNil)

			So(v.Message(), ShouldResemble, test_message)
			So(v.Queue(), ShouldEqual, test_queue)

			So(v.Id(), ShouldNotEqual, "")

			_, e := strconv.Atoi(v.Id())
			So(e, ShouldBeNil)

		})

		Convey(fmt.Sprintf("test publish message driver:%s", driverName), t, func() {
			test_queue := "test-publish-queue"
			test_message := []byte("testmessage")
			err := mq.Publish(test_queue, test_message)
			So(err, ShouldBeNil)
		})

		Convey(fmt.Sprintf("test publish message driver:%s", driverName), t, func() {
			data := []struct {
				queue    string
				messages [][]byte
			}{
				{"asdf", [][]byte{
					[]byte("message"), []byte("message2"),
				}},
			}

			for _, v := range data {
				for _, m := range v.messages {
					errPublish := mq.Publish(v.queue, m)
					So(errPublish, ShouldBeNil)
				}
			}

		})

	}
}
