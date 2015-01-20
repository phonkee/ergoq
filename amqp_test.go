package ergoq

import (
	"net/url"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"
)

func TestAmqp(t *testing.T) {

	Convey("test open", t, func() {
		_, err := amqp.Dial("amqp://nonexisting:guest@localhost:5672//test")
		So(err, ShouldNotBeNil)

	})
	Convey("test openconnection", t, func() {
		_, err := OpenConnection("amqp", nil)
		So(err, ShouldNotBeNil)

		x, errDial := amqp.Dial("amqp://guest:guest@localhost:5672//test")
		So(errDial, ShouldBeNil)

		_, errOpen := OpenConnection("amqp", x)
		So(errOpen, ShouldBeNil)

	})
}

func TestAmqpParseDSN(t *testing.T) {
	Convey("test parse dsn", t, func() {
		d, err := ParseAMQPDSN("amqp://guest:guest@localhost:5672/?reliable=true")
		So(err, ShouldBeNil)
		So(d.settings.reliable, ShouldBeTrue)

		d, err = ParseAMQPDSN("amqp://guest:guest@localhost:5672/?reliable=false")
		So(err, ShouldBeNil)
		So(d.settings.reliable, ShouldBeFalse)
	})
}

func TestAmqpURLSettings(t *testing.T) {
	Convey("test url settings", t, func() {

		var q url.Values
		var err error

		q, err = url.ParseQuery("reliable=true&auto_ack=true&prefix=ergoqprefix")
		So(err, ShouldBeNil)

		s := NewAMQPURLSettings(q)

		So(s.reliable, ShouldBeTrue)
		So(s.autoAck, ShouldBeTrue)
		So(s.prefix, ShouldEqual, "ergoqprefix")

		q, err = url.ParseQuery("")
		So(err, ShouldBeNil)
		s2 := NewAMQPURLSettings(q)

		So(s2.autoAck, ShouldEqual, DEFAULT_AUTO_ACK)
		So(s2.prefix, ShouldEqual, DEFAULT_PREFIX)
	})
}
