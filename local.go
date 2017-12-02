// local memory queue implementation.
// no ack, very simple useful for testing
package ergoq

import (
	"strconv"
	"sync"

	"github.com/phonkee/godsn"
)

const (
	defaultLocQueueSize = 1000
)

func init() {
	Register("loc", &locDriver{})
}

// locDriver represents driver for locQueue
type locDriver struct{}

// Open opens new connection based by given dsn
func (l *locDriver) Open(d string) (MessageQueuer, error) {

	// parse dsn
	DSN, err := godsn.Parse(d)

	if err != nil {
		return nil, err
	}

	// prepare topic
	result := &locQueue{
		mutex:  &sync.RWMutex{},
		size:   DSN.GetInt("size", defaultLocQueueSize),
		queues: make(map[string]chan QueueMessage),
		pubsub: make(map[string]map[chan SubscribeMessage]struct{}),
	}

	return result, nil
}

// OpenConnection
func (l *locDriver) OpenConnection(connection interface{}, settings string) (MessageQueuer, error) {
	return nil, nil
}

// locQueue is local memory implementation of MessageQueuer
type locQueue struct {
	// mutex to secure our topic and subscribers
	mutex *sync.RWMutex

	// size of topic
	size int

	// topic for push/pop messages
	queues map[string]chan QueueMessage

	// pubsub for publishing/subscribe to topics
	pubsub map[string]map[chan SubscribeMessage]struct{}
}

// Push message to topic
func (l *locQueue) Push(queue string, message []byte) error {

	// check if exists

	l.mutex.RLock()
	q, ok := l.queues[queue]
	l.mutex.RUnlock()

	if !ok {
		// now lock for write and do the check again (so other goroutine cannot accidentally be owerwritten)
		l.mutex.Lock()
		if _, okExists := l.queues[queue]; !okExists {
			l.queues[queue] = make(chan QueueMessage, l.size)
		}

		// assign to q
		q = l.queues[queue]
		l.mutex.Unlock()
	}

	// send message to topic in goroutine (non blocking)
	//go func() {
	// push message to topic
	q <- newLocQueueMessage(queue, message)
	//}()

	return nil
}

// Pop message from topic
func (l *locQueue) Pop(queue string) (QueueMessage, error) {

	l.mutex.RLock()
	q, ok := l.queues[queue]
	l.mutex.RUnlock()

	if !ok {
		return nil, nil
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	q, ok = l.queues[queue]

	if !ok {
		return nil, nil
	}

	if len(q) == 0 {
		return nil, nil
	}

	result := <-q

	return result, nil
}

// Publishes message to topic(fanout for all subscribers)
func (l *locQueue) Publish(topic string, message []byte) (err error) {

	l.mutex.RLock()
	mc, ok := l.pubsub[topic]

	if !ok {
		return
	}

	for key := range mc {
		key <- NewSubscriberMessage(topic, message)
	}
	l.mutex.RUnlock()

	return nil
}

// Subscribes to topic(s)
func (l *locQueue) Subscribe(quit <-chan struct{}, topics ...string) (chan SubscribeMessage, chan error) {
	result := make(chan SubscribeMessage, l.size)

	l.subscriberAdd(result, topics...)

	// run background goroutine that checks quit channel
	go func() {
		<-quit

		l.subscriberDelete(result, topics...)

		close(result)
	}()

	return result, make(chan error)
}

// add subscriber to given topic
func (l *locQueue) subscriberAdd(sc chan SubscribeMessage, topics ... string) {

	// locking involved

	l.mutex.Lock()
	defer l.mutex.Unlock()

	for _, topic := range topics {
		subscribers, ok := l.pubsub[topic]

		if !ok {
			l.pubsub[topic] = make(map[chan SubscribeMessage]struct{})
			subscribers = l.pubsub[topic]
		}

		subscribers[sc] = struct{}{}
	}
}

// delete subscriber from given topic
func (l *locQueue) subscriberDelete(sc chan SubscribeMessage, topics ... string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for _, topic := range topics {
		subscribers, ok := l.pubsub[topic]

		if !ok {
			return
		}

		delete(subscribers, sc)
	}
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//

// newLocQueueMessage creates new QueueMessage
func newLocQueueMessage(queue string, message []byte) QueueMessage {
	return &locQueueMessage{
		id:      NewID(),
		message: message,
		queue:   queue,
	}
}

// locQueueMessage local implementation of QueueMessage
type locQueueMessage struct {
	id      int64
	message []byte
	queue   string
}

func (l *locQueueMessage) Queue() string {
	return l.queue
}

func (l *locQueueMessage) Message() []byte {
	return l.message
}

func (l *locQueueMessage) Ack() error {
	return nil
}

func (l *locQueueMessage) Id() string {
	return strconv.FormatInt(l.id, 10)
}
