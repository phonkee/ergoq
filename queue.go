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

type MessageQueueDriverer interface {

	// "opens" message queuer
	Open(dsn string) (MessageQueuer, error)

	// Opens queuer by connection
	// 	settings is url encoded params e.g. "auto_ack=true&exchange=exchange"
	OpenConnection(connection interface{}, settings string) (MessageQueuer, error)
}

type MessageQueuer interface {

	// Pushes message to topic
	Push(queue string, message []byte) error

	// Pops message from topic
	Pop(queue string) (QueueMessage, error)

	// Publishes message to topic(fanout for all subscribers)
	Publish(topic string, message []byte) error

	// Subscribes to topic(s)
	Subscribe(quit <-chan struct{}, topics ...string) (chan SubscribeMessage, chan error)
}

var drivers = make(map[string]MessageQueueDriverer)

// Register makes a message topic driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver MessageQueueDriverer) {
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

// opens message topic by dsn
func Open(dsn string) (MessageQueuer, error) {
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

// opens message topic by name and connection
func OpenConnection(driverName string, connection interface{}, settings ...string) (MessageQueuer, error) {
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
