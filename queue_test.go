package ergoq

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

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
		"amqp://guest:guest@localhost:5672//test",
	}
	for _, dsn := range dsns {
		driverName, err := getNameFromDSN(dsn)
		if err != nil {
			t.Error(err)
		}

		Convey(fmt.Sprintf("test push/pop message driver:%s", driverName), t, func() {
			mq, err := Open(dsn)

			if err != nil {
				t.Fatalf("Error: %+v\n", err)
			}

			test_queue := "queue"
			test_message := []byte("message")

			errPush := mq.Push(test_queue, test_message)
			So(errPush, ShouldBeNil)

			//time.Sleep(time.Second * 2)
			v, errPop := mq.Pop(test_queue)
			So(errPop, ShouldBeNil)
			So(v, ShouldNotBeNil)

			So(v.Message(), ShouldResemble, test_message)
			So(v.Id(), ShouldNotEqual, "")

			errAck := v.Ack()
			So(errAck, ShouldBeNil)

			_, e := strconv.Atoi(v.Id())
			So(e, ShouldBeNil)
		})

		Convey(fmt.Sprintf("test publish message driver:%s", driverName), t, func() {
			mq, err := Open(dsn)

			if err != nil {
				t.Fatalf("Error: %+v\n", err)
			}

			test_queue := "somequeue"
			test_message := []byte("testmessage")
			err = mq.Publish(test_queue, test_message)
			So(err, ShouldBeNil)
		})

		Convey(fmt.Sprintf("test subscribe message driver:%s", driverName), t, func() {

			mq, err := Open(dsn)

			if err != nil {
				t.Fatalf("Error: %+v\n", err)
			}

			data := []struct {
				queue    string
				messages [][]byte
			}{
				{"anotherqueue", [][]byte{
					[]byte("message"), []byte("message2"),
				}},
				{"anotherqueue", [][]byte{
					[]byte("message3"), []byte("message4"),
				}},
			}

			for _, v := range data {
				quit := make(chan struct{})
				wg := &sync.WaitGroup{}
				gotData := [][]byte{}
				queue := v.queue
				results, _ := mq.Subscribe(quit, v.queue)
				countMessages := len(v.messages)
				wg.Add(countMessages)

				go func() {
					for i := 0; i < countMessages; i++ {
						q := <-results
						gotData = append(gotData, q.Message())
						So(q.Queue(), ShouldEqual, queue)
						wg.Done()
					}
				}()

				for _, m := range v.messages {
					errPublish := mq.Publish(queue, m)
					So(errPublish, ShouldBeNil)
				}

				wg.Wait()
				So(gotData, ShouldResemble, v.messages)
			}

		})

	}
}
