package ergoq

import (
	"fmt"
	"sort"
)

type MessageQueueDriver interface {

	// "opens" message queuer
	Open(dsn string) (MessageQueue, error)

	// Opens queuer by connection
	OpenConnection(connection interface{}) (MessageQueue, error)
}

type MessageQueue interface {

	// Opens connection
	// Open(dsn string) error

	// Pushes message to queue
	// Direct
	Push(queue string, messages ...[]byte) error

	// Pops message from queue
	Pop(queue string) (QueueMessage, error)

	// Publishes message to topic
	// Fanout
	Publish(queue string, message []byte) error

	// Subscribes to queue(s)
	Subscribe(quit <-chan struct{}, topics ...string) (chan SubscribeMessage, chan error)
}

var drivers = make(map[string]MessageQueueDriver)

// Register makes a message queue driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver MessageQueueDriver) {
	if driver == nil {
		panic("sql: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("sql: Register called twice for driver " + name)
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
func OpenConnection(driverName string, connection interface{}) (MessageQueue, error) {
	di, ok := drivers[driverName]
	if !ok {
		return nil, fmt.Errorf("ergoq: unknown driver %q (forgotten import?)", driverName)
	}

	return di.OpenConnection(connection)
}
