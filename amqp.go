// AMQP driver implementation
package ergoq

import (
	"fmt"
	"net/url"

	"github.com/streadway/amqp"
)

const (
	RELIABLE_URL_PARAM = "reliable"
	AUTO_ACK_URL_PARAM = "auto_ack"

	DEF_AMQP_RELIABLE = true
	DEF_AMQP_AUTO_ACK = false
)

func init() {
	Register("amqp", &AMQPDriver{})
}

type AMQPDriver struct{}

// Open by DSN
func (a *AMQPDriver) Open(dsn string) (MessageQueue, error) {

	pd, err := ParseAMQPDSN(dsn)
	if err != nil {
		return nil, err
	}
	conn, errDial := amqp.Dial(pd.dsn)

	if errDial != nil {
		return nil, errDial
	}

	d := amqpMessageQueue{
		conn:    conn,
		autoAck: pd.autoAck,
	}

	return &d, nil
}

// open by already instantiated connection
func (a *AMQPDriver) OpenConnection(connection interface{}) (MessageQueue, error) {

	conn, ok := connection.(*amqp.Connection)
	if !ok {
		return nil, fmt.Errorf("ergoq: amqp connection %+v not recognized.", connection)
	}

	d := amqpMessageQueue{
		conn:    conn,
		autoAck: DEF_AMQP_AUTO_ACK,
	}

	return &d, nil
}

// amqp message queue implementation
type amqpMessageQueue struct {
	conn    *amqp.Connection
	autoAck bool
}

// pushes message to queue
// TODO: add "mandatory", "immediate" and other settings to parsedsn?
func (a *amqpMessageQueue) Push(queue string, messages ...[]byte) (err error) {
	if len(messages) == 0 {
		return fmt.Errorf("no messages given")
	}
	ch, errChannel := a.conn.Channel()
	if errChannel != nil {
		return errChannel
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		queue,    // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return err
	}

	for _, message := range messages {
		err = ch.Publish(
			"",    // exchange
			queue, // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         message,
			})
		if err != nil {

			return err
		}
	}

	return nil
}
func (a *amqpMessageQueue) Pop(queue string) (QueueMessage, error) {
	ch, errChannel := a.conn.Channel()
	if errChannel != nil {
		return nil, errChannel
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	err = ch.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		return nil, err
	}

	result, _, errGet := ch.Get(q.Name, a.autoAck)
	if errGet != nil {
		return nil, errGet
	}

	message := amqpQueueMessage{
		delivery: &result,
		autoAck:  a.autoAck,
	}

	return &message, nil
}
func (a *amqpMessageQueue) Publish(queue string, message []byte) (err error) {
	ch, errChannel := a.conn.Channel()
	if errChannel != nil {
		return errChannel
	}

	defer ch.Close()

	err = ch.ExchangeDeclare(
		queue,   // name
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)

	if err != nil {
		return err
	}

	err = ch.Publish(
		queue, // exchange
		"",    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		return err
	}

	return nil
}

// starts receiving messages
func (a *amqpMessageQueue) Subscribe(quit <-chan struct{}, queues ...string) (chan SubscribeMessage, chan error) {
	ch, errChannel := a.conn.Channel()
	if errChannel != nil {
		e := make(chan error)
		e <- errChannel
		return nil, e
	}

	// defer ch.Close()

	receivers := make([]<-chan amqp.Delivery, 0, len(queues))
	errors := make(chan error)
	values := make(chan SubscribeMessage)

	for _, queue := range queues {
		msgs, err := ch.Consume(
			queue, // queue
			"",    // consumer
			true,  // auto ack
			false, // exclusive
			false, // no local
			false, // no wait
			nil,   // args
		)
		if err != nil {
			continue
		}
		receivers = append(receivers, msgs)
	}

	go func() {
		for {
			select {
			case <-quit:
				// what now?
				// close queuest or do something else??
			}

			for _, m := range receivers {
				select {
				case msg := <-m:
					values <- NewSubscriberMessage(msg.Exchange, msg.Body)
				}
			}
		}
	}()

	return values, errors
}

// dsn information
type AMQPDSN struct {
	reliable bool
	dsn      string
	url      *url.URL
	autoAck  bool
}

// Parses dsn
func ParseAMQPDSN(dsn string) (*AMQPDSN, error) {
	p, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	d := &AMQPDSN{
		url:      p,
		reliable: GetBool(p.Query(), RELIABLE_URL_PARAM, DEF_AMQP_RELIABLE),
		autoAck:  GetBool(p.Query(), AUTO_ACK_URL_PARAM, DEF_AMQP_AUTO_ACK),
		dsn:      dsn,
	}

	return d, nil
}

// Messages
type amqpQueueMessage struct {
	autoAck  bool
	delivery *amqp.Delivery
}

// returns message content
func (a *amqpQueueMessage) Message() []byte {
	return a.delivery.Body
}

// Acknowledge message
func (a *amqpQueueMessage) Ack() error {
	if a.autoAck {
		return fmt.Errorf("message automatically acknowledged")
	}

	// acknowledge only single message thus false
	a.delivery.Ack(false)
	return nil
}

// returns id of message
func (a *amqpQueueMessage) Id() string {
	// MessageId is blank, amqp library error?
	return fmt.Sprintf("%d", a.delivery.MessageCount)
}

// returns queue
func (a *amqpQueueMessage) Queue() string {
	return a.delivery.RoutingKey
}
