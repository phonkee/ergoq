package ergoq

// QueueMessage interface
type QueueMessage interface {
	// queue
	Queue() string

	// returns contents of message
	Message() []byte

	// Acknowledges message
	Ack() error

	// returns id of message
	Id() string
}

// SubscribeMessage interface
type SubscribeMessage interface {
	// returns queue where was message published
	Queue() string

	// returns content of message
	Message() []byte
}

func NewSubscriberMessage(topic string, message []byte) SubscribeMessage {
	return &subscriberMessage{
		queue:   topic,
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
