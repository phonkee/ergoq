package ergoq

func init() {
	// Register("amqp", &AMQPDriver{})
}

type AMQPDriver struct{}

// Open by DSN
func (a *AMQPDriver) Open(dsn string) (MessageQueue, error) {
	return nil, nil
}

// open by already instantiated connection
func (a *AMQPDriver) OpenConnection(connection interface{}) (MessageQueue, error) {
	return nil, nil
}

// amqp message queue implementation
type AMQPMessageQueue struct{}
