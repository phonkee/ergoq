package ergoq

import (
	"math/rand"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	redisDsn = "redis://localhost:6379"
)

func TestRedis(t *testing.T) {

	Convey("test OpenConnection", t, func() {
		pool := redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", ":6379")
			},
		}
		var err error
		_, err = OpenConnection("redis", &pool)
		So(err, ShouldBeNil)

		_, err = OpenConnection("redis", nil)
		So(err, ShouldNotBeNil)

		_, err = Open(".......")
		So(err, ShouldNotBeNil)
	})

	Convey("Test parseDSN", t, func() {
		p, err := parseDSN("redis://guest:pass@localhost:6379?max_idle=555&max_active=7890&idle_timeout=4321")
		So(err, ShouldBeNil)
		So(p.Database, ShouldEqual, -1)
		So(p.Host, ShouldEqual, "localhost:6379")
		So(p.Password, ShouldEqual, "pass")
		So(p.PoolMaxIdle, ShouldEqual, 555)
		So(p.PoolMaxActive, ShouldEqual, 7890)
		So(p.PoolIdleTimeout, ShouldEqual, 4321)
		So(p.RetryNonAckedTimeout, ShouldEqual, RETRY_NON_ACKED_TIMEOUT)
		So(p.AutoAck, ShouldEqual, AUTO_ACK)

		p, err = parseDSN("redis://guest:pass@localhost:6379/21?max_idle=555&max_active=7890&idle_timeout=4321&retry_non_acked_timeout=9876&auto_ack=false")
		So(err, ShouldBeNil)
		So(p.Database, ShouldEqual, 21)
		So(p.RetryNonAckedTimeout, ShouldEqual, 9876)
		So(p.AutoAck, ShouldEqual, false)

		_, err = parseDSN("redis://guest:pass@localhost:6379/invalid?max_idle=555&max_active=7890&idle_timeout=4321")
		So(err, ShouldNotBeNil)

		_, errParse := parseDSN("http://%")
		So(errParse, ShouldNotBeNil)
	})

	Convey("Test custom scripts", t, func() {
		timestamp := time.Now().Unix()
		_ = timestamp

		mq, err := Open(redisDsn)
		So(err, ShouldBeNil)

		rmq := mq.(*redisMessageQueue)
		_ = rmq

		conn := rmq.pool.Get()
		defer conn.Close()

	})

	Convey("Test ack", t, func() {
		mq, err := Open("redis://localhost:6379?retry_non_acked_timeout=2&auto_ack=false")
		So(err, ShouldBeNil)

		test_queue := "test-queue"
		test_message := []byte("test-message" + string(rand.Intn(1000000)))

		errPush := mq.Push(test_queue, test_message)
		So(errPush, ShouldBeNil)

		v, e := mq.Pop(test_queue)
		So(e, ShouldBeNil)

		// after 5 seconds should be re-queued
		time.Sleep(time.Second * 5)

		v2, e2 := mq.Pop(test_queue)
		So(e2, ShouldBeNil)

		So(v.Message(), ShouldResemble, v2.Message())
		So(v2.Ack(), ShouldBeNil)

		mq2, err2 := Open("redis://localhost:6379?auto_ack=true")
		So(err2, ShouldBeNil)

		mq2.Push(test_queue, test_message)
		v3, errPop := mq2.Pop(test_queue)
		So(errPop, ShouldBeNil)
		So(v3.Ack(), ShouldBeNil)

	})

}
