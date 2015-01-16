package ergoq

// QueueMessage interface
type QueueMessage interface {
	// queue
	Queue() string

	// returns contents of message
	Message() []byte

	// Acknowledges message
	Ack() error

	// returns id int
	Id() string
}

// SubscribeMessage interface
type SubscribeMessage interface {
	// returns queue where was message published
	Queue() string

	// returns content of message
	Message() []byte
}

func NewSubscriberMessage(queue string, message []byte) SubscribeMessage {
	return &subscriberMessage{
		queue:   queue,
		message: message,
	}
}

type subscriberMessage struct {
	queue   string
	message []byte
}

func (s *subscriberMessage) Queue() string {
	return s.queue
}

func (s *subscriberMessage) Message() []byte {
	return s.message
}
