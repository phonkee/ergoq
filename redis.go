package ergoq

import (
	"bytes"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	// default values for redisMessageQueue
	MAX_IDLE                = 10
	MAX_ACTIVE              = 10
	IDLE_TIMEOUT            = 500
	RETRY_NON_ACKED_TIMEOUT = 600
	AUTO_ACK                = false
	REQUEUE_NON_ACKED_NUM   = 10
)

// register redis driver
func init() {
	Register("redis", &redisDriver{})
}

// Redis driver implementation
type redisDriver struct{}

// opens message queue by dsn
func (r *redisDriver) Open(dsn string) (MessageQueue, error) {
	pd, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	// convert to duration
	idleTimeout := time.Duration(pd.PoolIdleTimeout) * time.Millisecond

	// Instantiate redis pool
	pool := redis.Pool{
		MaxIdle:     pd.PoolMaxIdle,
		MaxActive:   pd.PoolMaxActive,
		IdleTimeout: idleTimeout,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial(pd.Network, pd.Host)
			if err != nil {
				return nil, err
			}
			if pd.Database > -1 {
				_, errSelect := conn.Do("SELECT", pd.Database)
				if errSelect != nil {
					return nil, errSelect
				}
			}
			return conn, nil
		},
	}

	mq := newRedisMessageQueue(&pool)
	mq.retryNonAckedTimeout = pd.RetryNonAckedTimeout
	mq.autoAck = pd.AutoAck
	return mq, nil
}

// opens message queue by connection (&redis.Pool)
func (r *redisDriver) OpenConnection(connection interface{}, settings string) (MessageQueue, error) {
	switch connection.(type) {
	case *redis.Pool:
		pool := connection.(*redis.Pool)
		mq := newRedisMessageQueue(pool)
		mq.retryNonAckedTimeout = RETRY_NON_ACKED_TIMEOUT
		return mq, nil
	default:
		return nil, fmt.Errorf("Connection %s is unknown.", connection)
	}
}

func newRedisMessageQueue(pool *redis.Pool) *redisMessageQueue {
	mq := redisMessageQueue{
		pool: pool,
	}
	return &mq
}

// redis message queue is redis implementation of MessageQueue interface
type redisMessageQueue struct {
	// redis pool
	pool *redis.Pool

	// timeout to mark as non acked
	retryNonAckedTimeout int

	// automatically acknowledge of message
	autoAck bool
}

// Pushes message to queue to be consumed by one of workers
func (r *redisMessageQueue) Push(queue string, message []byte) error {
	// get connection from pool
	conn := r.pool.Get()
	// release connection back to pool
	defer conn.Close()

	_, err := conn.Do("LPUSH", redis.Args{}.Add(queue).Add(message)...)
	return err
}

// Pops message from queue
func (r *redisMessageQueue) Pop(queue string) (QueueMessage, error) {
	// get connection from pool
	conn := r.pool.Get()

	timestamp := time.Now().Unix()

	_, _ = queueNonAckedScript.Do(conn, queue, timestamp, r.retryNonAckedTimeout, 10)

	// auto acknowledge mode
	if r.autoAck {
		message, err := redis.Bytes(conn.Do("LPOP", queue))
		if err != nil {
			return nil, err
		}
		return &redisQueueMessage{
			message: message,
			autoAck: true,
			queue:   queue,
			id:      "",
		}, nil
	}

	values, err := redis.Values(popScript.Do(conn, queue, timestamp))
	if err != nil {
		return nil, err
	}

	// scan values from result
	q, _ := redis.String(values[0], nil)
	message, _ := redis.Bytes(values[1], nil)
	id, _ := redis.String(values[2], nil)

	return &redisQueueMessage{
		message: message,
		autoAck: r.autoAck,
		queue:   q,
		id:      id,
		pool:    r.pool,
	}, nil
}

// publishes to all queue listeners
func (r *redisMessageQueue) Publish(queue string, message []byte) error {
	// get connection from pool
	conn := r.pool.Get()
	// release connection back to pool
	defer conn.Close()

	_, err := conn.Do("PUBLISH", queue, message)
	return err
}

// Subscribe
func (r *redisMessageQueue) Subscribe(quit <-chan struct{}, queues ...string) (chan SubscribeMessage, chan error) {

	results := make(chan SubscribeMessage)
	errors := make(chan error)

	conn := r.pool.Get()

	pubsubConn := redis.PubSubConn{conn}
	pubsubConn.Subscribe(redis.Args{}.AddFlat(queues)...)

	go func() {
		// release connection back to pool
		defer conn.Close()

		go func() {
			for {
				select {
				case _ = <-quit:
					pubsubConn.Unsubscribe()
					return
				}
			}
		}()

		for {
			switch v := pubsubConn.Receive().(type) {
			case redis.Message:
				results <- NewSubscriberMessage(v.Channel, v.Data)
			case redis.Subscription:
				if v.Kind == "unsubscribe" {
					return
				}
			case error:
				// Really needed to unsubscribe?
				pubsubConn.Unsubscribe()
				errors <- v.(error)
				return
			}
		}

	}()

	return results, errors
}

// RedisQueueMessage
type redisQueueMessage struct {
	message []byte
	autoAck bool
	queue   string
	id      string
	pool    *redis.Pool
}

// returns message content
func (r *redisQueueMessage) Message() []byte {
	return r.message
}

// Acknowledge message
func (r *redisQueueMessage) Ack() error {
	// if auto ack is set call to Ack is not error, it is?
	if r.autoAck {
		// return fmt.Errorf("message automatically acknowledged")
		return nil
	}
	conn := r.pool.Get()
	defer conn.Close()

	fm := bytes.NewBufferString(r.Id() + ":")
	fm.Write(r.Message())

	_, err := conn.Do("ZREM", r.Queue(), fm.Bytes())
	return err
}

// returns id of message
func (r *redisQueueMessage) Id() string {
	return r.id
}

// returns queue
func (r *redisQueueMessage) Queue() string {
	return r.queue
}

// parseDSN
type parsedDSN struct {
	Network              string
	Host                 string
	Database             int
	Password             string
	PoolMaxActive        int
	PoolMaxIdle          int
	PoolIdleTimeout      int
	RetryNonAckedTimeout int
	AutoAck              bool
}

// returns default values
func defaultParsedDSN() parsedDSN {
	return parsedDSN{
		PoolMaxActive:        MAX_ACTIVE,
		PoolMaxIdle:          MAX_IDLE,
		PoolIdleTimeout:      IDLE_TIMEOUT,
		Network:              "tcp",
		RetryNonAckedTimeout: RETRY_NON_ACKED_TIMEOUT,
		AutoAck:              false,
	}
}

// Parses dsn and returns struct
func parseDSN(dsn string) (*parsedDSN, error) {
	p, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	pd := defaultParsedDSN()
	pd.Host = p.Host

	if p.User != nil {
		pass, ok := p.User.Password()
		if ok {
			pd.Password = pass
		}
	}

	tp := strings.Trim(p.Path, "/")

	if tp != "" {
		db, errDb := strconv.Atoi(tp)
		if errDb != nil {
			return nil, fmt.Errorf("Cannot parse db number from %s", dsn)
		}

		if db >= 0 {
			pd.Database = db
		}
	} else {
		pd.Database = -1
	}

	urlValues := p.Query()
	pd.PoolMaxActive = GetInt(urlValues, "max_active", pd.PoolMaxActive)
	pd.PoolMaxIdle = GetInt(urlValues, "max_idle", pd.PoolMaxIdle)
	pd.PoolIdleTimeout = GetInt(urlValues, "idle_timeout", pd.PoolIdleTimeout)
	pd.RetryNonAckedTimeout = GetInt(urlValues, "retry_non_acked_timeout", pd.RetryNonAckedTimeout)
	pd.AutoAck = GetBool(urlValues, "auto_ack", AUTO_ACK)

	return &pd, nil
}
