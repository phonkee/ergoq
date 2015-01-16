# README #

## What is this repository for? ##

Ergoq package is small and lightweight message queue abstraction.
Currently only redis implementation is done, but amqp is on the way also.


## Usage ##

All snippets of code assume import of library

	import (
		"github.com/phonkee/ergoq"
	)


Ergoq supports drivers system as seen in sql package. Every driver uses it's own connection(for redis it's redis.Pool).
To open ergoq message queue you can use Open function and provide DSN. 
Every driver can have slightly different implementation but usually you will see

	<driverName>://<host>:<port>/<database>?params
	<driverName>://<socket>/<database>?params

Example:

```
#!go
dsn := "redis://localhost:6379/0?max_idle=100&max_active=100&idle_timeout=200"
```

Each driver can support it's params. 

#### Drivers ####

RedisMessageQueueDriver

connection: &redis.Pool

DSN params:

* max_idle - default is 10
* max_active - default is 10
* idle_timeout - default is 300




## Open message queue ##

You can open message two ways. 

a. You provide DSN string to ergoq.Open and let ergoq make connections for you

```
#!go
mq, err := ergoq.Open("redis://localhost:6379/0")
if err != nil {
	panic(err)
}
```

b. You provide connection to OpenConnection

```
#!go
pool := redis.Pool{
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	},
}
mq, err := ergoq.OpenConnection("redis", &pool)
if err != nil {
	panic(err)
}
```

## API ##

MessageQueue interface says it all.

```
#!go
type MessageQueue interface {
	// Pushes message to queue
	// Direct
	Push(queue string, messages ...[]byte) error

	// Pops message from queue
	// In case of blocking == True timeout can be set or default will be used
	Pop(queue string) ([]byte, error)

	// Publishes message to topic
	// Fanout
	Publish(queue string, message []byte) error

	// Subscribes to queue(s)
	Subscribe(quit <-chan struct{}, topics ...string) (chan SubscriberMessage, chan error)
}
```


Examples:

```
#!go
// Error checking is omitted, but please you make you checks!
mq, _ := ergoq.Open("redis://localhost:6379/0")

// If we want to push to queue (direct) only first who pops this value will
// process it
_ := mq.Push("queue", []byte("message"))

// pop data from queue
// second argument is blocking
// third optional parameter is timeout for blocking
data, _ := mq.Pop("queue")

// If we want to publish message to all subscribers of given queue
// we need to call Publish method

errPub := mq.Publish("user:1", "logged_in")
if errPub != nil {
	panic(errPub)
}

// subscribe to channels can be donw following way.
// You need to provide "quit" channel when subscription will be stopped.
// Subscribe returns 2 channels, result and errors.
quit := make(chan struct{})
results, error := mq.Subscribe(quit, "user:1", "admins")

go func() {
	for {
		select {
			r <- results:
				fmt.Println("result %+v", r)
			e <- errors:
				panic(e)
		}
	}
}()
```

### Contribute ###

Welcome!