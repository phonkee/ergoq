package ergoq

import (
	"fmt"
	"sort"
)

const (
	URL_PARAM_NAME_AUTO_ACK = "auto_ack"
	URL_PARAM_PREFIX        = "prefix"

	// default values
	DEFAULT_AUTO_ACK = false
	DEFAULT_PREFIX   = "errgoq"
)

type MessageQueueDriver interface {

	// "opens" message queuer
	Open(dsn string) (MessageQueue, error)

	// Opens queuer by connection
	// 	settings is url encoded params e.g. "auto_ack=true&exchange=exchange"
	OpenConnection(connection interface{}, settings string) (MessageQueue, error)
}

type MessageQueue interface {

	// Pushes message to queue
	Push(queue string, message []byte) error

	// Pops message from queue
	Pop(queue string) (QueueMessage, error)

	// Publishes message to queue(fanout for all subscribers)
	Publish(queue string, message []byte) error

	// Subscribes to queue(s)
	Subscribe(quit <-chan struct{}, queues ...string) (chan SubscribeMessage, chan error)
}

var drivers = make(map[string]MessageQueueDriver)

// Register makes a message queue driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver MessageQueueDriver) {
	if driver == nil {
		panic("ergoq: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("ergoq: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

// returns list of available driver names
func Drivers() []string {
	var list []string
	for name := range drivers {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

// opens message queue by dsn
func Open(dsn string) (MessageQueue, error) {
	driverName, err := getNameFromDSN(dsn)
	if err != nil {
		return nil, err
	}
	di, ok := drivers[driverName]
	if !ok {
		return nil, fmt.Errorf("ergoq: unknown driver %q (forgotten import?)", driverName)
	}

	return di.Open(dsn)
}

// opens message queue by name and connection
func OpenConnection(driverName string, connection interface{}, settings ...string) (MessageQueue, error) {
	di, ok := drivers[driverName]
	if !ok {
		return nil, fmt.Errorf("ergoq: unknown driver %q (forgotten import?)", driverName)
	}
	// additional settings
	urlSettings := ""
	if len(settings) > 0 {
		urlSettings = settings[0]
	}

	return di.OpenConnection(connection, urlSettings)
}
