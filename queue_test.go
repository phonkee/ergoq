package ergoq

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
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
		//"redis://localhost:6379",
		//"amqp://guest:guest@localhost:5672//test",
		"loc:///size=1000",
	}

	conn, _ := redis.Dial("tcp", "localhost:6379")
	conn.Do("FLUSHDB")

	for _, dsn := range dsns {
		driverName, err := getNameFromDSN(dsn)
		if err != nil {
			t.Error(err)
		}

		Convey(fmt.Sprintf("test push/pop message driver:%s", driverName), t, func() {
			mq, err := Open(dsn)

			if err != nil {
				t.Fatalf("Error: %+v\n", err)
				t.Fail()
			}

			if mq == nil {
				t.Fatalf("Driver returned nil error and nil MessageQueuer: %+v\n", dsn)
				t.Fail()
			}

			test_queue := "topic"
			test_message := []byte("message" + string(rand.Intn(1000000)))

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
			test_message := []byte("testmessage" + string(rand.Intn(1000000)))
			err = mq.Publish(test_queue, test_message)
			So(err, ShouldBeNil)
		})

		// commented due to error in goconvey (context in goroutines)
		Convey(fmt.Sprintf("test subscribe message driver:%s", driverName), t, func(c C) {

			mq, err := Open(dsn)

			if err != nil {
				t.Fatalf("Error: %+v\n", err)
			}

			data := []struct {
				topic    string
				messages [][]byte
			}{
				{"anotherqueue", [][]byte{
					[]byte("message" + string(rand.Intn(1000000))), []byte("message2" + string(rand.Intn(1000000))),
				}},
				{"another", [][]byte{
					[]byte("message3" + string(rand.Intn(1000000))), []byte("message4" + string(rand.Intn(1000000))),
				}},
			}

			wg := &sync.WaitGroup{}

			for _, v := range data {
				func(c2 C) {
					wg.Add(1)
					defer wg.Done()

					quit := make(chan struct{})
					results, _ := mq.Subscribe(quit, v.topic)

					countMessages := len(v.messages)

					for i := 0; i < countMessages; i++ {
						errPublish := mq.Publish(v.topic, v.messages[i])
						c.So(errPublish, ShouldBeNil)
					}

					close(quit)

					time.Sleep(time.Millisecond * 500)
					So(len(results), ShouldEqual, countMessages)
				}(c)

			}

			wg.Wait()
		})

	}
}
