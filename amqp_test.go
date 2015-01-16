package ergoq

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAmqpParseDSN(t *testing.T) {
	Convey("test parse dsn", t, func() {
		d, err := ParseAMQPDSN("amqp://guest:guest@localhost:5672/?reliable=true")
		So(err, ShouldBeNil)
		So(d.reliable, ShouldBeTrue)

		d, err = ParseAMQPDSN("amqp://guest:guest@localhost:5672/?reliable=false")
		So(err, ShouldBeNil)
		So(d.reliable, ShouldBeFalse)
	})
}
