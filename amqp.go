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
	EXCHANGE_URL_PARAM = "exchange"

	DEF_AMQP_RELIABLE = true
	DEF_AMQP_AUTO_ACK = false
	DEF_AMQP_PREFIX   = "errgoq"
	DEF_EXCHANGE      = "errgo"
)

func init() {
	Register("amqp", &AMQPDriver{})
}

type AMQPDriver struct{}

// Open by DSN
func (a *AMQPDriver) Open(dsn string) (MessageQueuer, error) {

	pd, err := ParseAMQPDSN(dsn)
	if err != nil {
		return nil, err
	}
	conn, errDial := amqp.Dial(pd.dsn)

	if errDial != nil {
		return nil, errDial
	}

	d := amqpMessageQueue{
		conn:     conn,
		settings: pd.settings,
	}

	return &d, nil
}

// open by already instantiated connection
func (a *AMQPDriver) OpenConnection(connection interface{}, settings string) (MessageQueuer, error) {

	conn, ok := connection.(*amqp.Connection)
	if !ok {
		return nil, fmt.Errorf("ergoq: amqp connection %+v not recognized.", connection)
	}

	s, err := url.ParseQuery(settings)
	if err != nil {
		return nil, err
	}

	d := amqpMessageQueue{
		conn:     conn,
		settings: NewAMQPURLSettings(s),
	}

	return &d, nil
}

// amqp message queue implementation
type amqpMessageQueue struct {
	conn     *amqp.Connection
	settings *amqpURLSettings
}

func (a *amqpMessageQueue) declareFanoutExchange(channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		a.settings.getName("fanout"), // name
		"direct",                     // type
		false,                        // durable
		true,                         // auto-deleted
		false,                        // internal
		false,                        // no-wait
		nil,                          // arguments
	)
}

// pushes message to queue
func (a *amqpMessageQueue) Push(queue string, message []byte) (err error) {
	ch, err := a.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, errQueue := ch.QueueDeclare(
		a.settings.getName(queue),
		true,
		true,
		false,
		false,
		nil)
	if errQueue != nil {
		return errQueue
	}

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         message,
		})
	if err != nil {
		return err
	}

	return nil
}

// Pop message from direct exchange, in case of settings.autoAck = true
// 	Queue message must be acknowledged
func (a *amqpMessageQueue) Pop(queue string) (QueueMessage, error) {
	ch, errChannel := a.conn.Channel()
	if errChannel != nil {
		return nil, errChannel
	}

	if a.settings.autoAck {
		defer ch.Close()
	}

	q, errQueue := ch.QueueDeclare(
		a.settings.getName(queue),
		true,
		true,
		false,
		false,
		nil)
	if errQueue != nil {
		return nil, errQueue
	}

	d, ok, e := ch.Get(q.Name, a.settings.autoAck)
	if e != nil {
		return nil, e
	}
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	message := &amqpQueueMessage{
		settings: a.settings,
		delivery: &d,
		channel:  ch,
	}

	return message, nil
}

// publishes message to fanout exchange
func (a *amqpMessageQueue) Publish(queue string, message []byte) (err error) {
	ch, errChannel := a.conn.Channel()

	if errChannel != nil {
		return errChannel
	}

	err = a.declareFanoutExchange(ch)
	if err != nil {
		return err
	}

	err = ch.Publish(
		a.settings.getName("fanout"), // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		return err
	}

	return err
}

// starts receiving messages
func (a *amqpMessageQueue) Subscribe(quit <-chan struct{}, queues ...string) (chan SubscribeMessage, chan error) {
	errors := make(chan error)
	values := make(chan SubscribeMessage)
	ch, _ := a.conn.Channel()

	a.declareFanoutExchange(ch)

	q, _ := ch.QueueDeclare(
		"",
		false, //durable
		true,  // autodelete
		false, // exclusive
		false, // nowait
		nil)   //args

	fe := a.settings.getName("fanout")
	// bind to multiple routing keys
	for _, queue := range queues {
		ch.QueueBind(
			q.Name, // queue name
			queue,  // routing key
			fe,     // exchange
			false,  //nowait
			nil)    // args
	}

	deliveries, _ := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // autoack
		true,   // exclusive
		false,  // nonlocal
		false,  // nowait
		nil)    //args

	go func() {
		for {
			select {
			case d := <-deliveries:
				sm := NewSubscriberMessage(d.RoutingKey, d.Body)
				values <- sm
			case <-quit:
				ch.Close()
				close(values)
				close(errors)
			}
		}
	}()

	return values, errors
}

// amqpURLSettings
type amqpURLSettings struct {
	// TODO: implement reliable messaging
	reliable bool
	autoAck  bool
	prefix   string
}

// returns prefixed exchange name
func (a *amqpURLSettings) getName(name string) string {
	return a.prefix + "." + name
}

func NewAMQPURLSettings(values url.Values) *amqpURLSettings {
	settings := &amqpURLSettings{
		reliable: GetBool(values, RELIABLE_URL_PARAM, DEF_AMQP_RELIABLE),
		autoAck:  GetBool(values, URL_PARAM_NAME_AUTO_ACK, DEFAULT_AUTO_ACK),
		prefix:   GetString(values, URL_PARAM_PREFIX, DEFAULT_PREFIX),
	}
	return settings
}

// dsn information
type AMQPDSN struct {
	dsn      string
	url      *url.URL
	settings *amqpURLSettings
}

// Parses dsn
func ParseAMQPDSN(dsn string) (*AMQPDSN, error) {
	p, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	d := &AMQPDSN{
		url:      p,
		dsn:      dsn,
		settings: NewAMQPURLSettings(p.Query()),
	}

	return d, nil
}

// Messages
type amqpQueueMessage struct {
	settings *amqpURLSettings
	delivery *amqp.Delivery
	channel  *amqp.Channel
}

// returns message content
func (a *amqpQueueMessage) Message() []byte {
	return a.delivery.Body
}

// Acknowledge message
func (a *amqpQueueMessage) Ack() error {
	if a.settings.autoAck {
		return fmt.Errorf("message automatically acknowledged")
	}

	// acknowledge only single message thus false
	err := a.delivery.Ack(false)
	a.channel.Close()
	return err
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
