package mercure

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

const (
	defaultRedisStreamName       = "updates"
	defaultRedisSubscribersSize  = 100000
	defaultRedisStreamBatch      = 1000
	defaultRedisHistoryLimit     = 0
	defaultRedisCleanupFrequency = 0.3
)

func init() {
	RegisterTransportFactory("redis", NewRedisTransport)
}

type RedisTransport struct {
	sync.RWMutex
	subscribers      *SubscriberList
	logger           Logger
	client           *redis.Client
	stream           string
	streamBatch      int
	historyLimit     int
	subscribersSize  int
	cleanupFrequency float64
	closed           bool
}

func NewRedisTransport(transportUrl *url.URL, logger Logger) (Transport, error) {
	var err error
	q := transportUrl.Query()

	streamName := defaultRedisStreamName
	if q.Get("stream_name") != "" {
		streamName = q.Get("stream_name")
	}

	historyLimit := defaultRedisHistoryLimit
	if historyLimitParameter := q.Get("history_limit"); historyLimitParameter != "" {
		historyLimit, err = strconv.Atoi(historyLimitParameter)
		if err != nil {
			return nil, &TransportError{transportUrl.Redacted(), fmt.Sprintf(`invalid "history_limit" parameter %q`, historyLimit), err}
		}
	}

	subscribersSize := defaultRedisSubscribersSize
	if subscribersSizeParameter := q.Get("subscribers_size"); subscribersSizeParameter != "" {
		subscribersSize, err = strconv.Atoi(subscribersSizeParameter)
		if err != nil {
			return nil, &TransportError{transportUrl.Redacted(), fmt.Sprintf(`invalid "subscribers_size" parameter %q`, subscribersSize), err}
		}
	}

	streamBatch := defaultRedisStreamBatch
	if streamBatchParameter := q.Get("stream_batch"); streamBatchParameter != "" {
		streamBatch, err = strconv.Atoi(streamBatchParameter)
		if err != nil {
			return nil, &TransportError{transportUrl.Redacted(), fmt.Sprintf(`invalid "stream_batch" parameter %q`, subscribersSize), err}
		}
	}

	cleanupFrequency := defaultRedisCleanupFrequency
	cleanupFrequencyParameter := q.Get("cleanup_frequency")
	if cleanupFrequencyParameter != "" {
		cleanupFrequency, err = strconv.ParseFloat(cleanupFrequencyParameter, 64)
		if err != nil {
			return nil, &TransportError{transportUrl.Redacted(), fmt.Sprintf(`invalid "cleanup_frequency" parameter %q`, cleanupFrequencyParameter), err}
		}
	}

	credentials := transportUrl.User
	if credentials == nil {
		return nil, &TransportError{transportUrl.Redacted(), "missing credentials", err}
	}
	password, _ := credentials.Password()

	address := transportUrl.Host
	if address == "" {
		return nil, &TransportError{transportUrl.Redacted(), "missing host", err}
	}

	client := redis.NewClient(&redis.Options{
		Username: credentials.Username(),
		Password: password,
		Addr:     address,
	})

	return NewRedisTransportInstance(logger, client, streamName, historyLimit, subscribersSize, streamBatch, cleanupFrequency)
}

func NewRedisTransportInstance(
	logger Logger,
	client *redis.Client,
	stream string,
	historyLimit int,
	subscribersSize int,
	streamBatch int,
	cleanupFrequency float64,
) (*RedisTransport, error) {
	return &RedisTransport{
		logger:           logger,
		client:           client,
		stream:           stream,
		historyLimit:     historyLimit,
		cleanupFrequency: cleanupFrequency,
		subscribersSize:  subscribersSize,
		streamBatch:      streamBatch,
		subscribers:      NewSubscriberList(subscribersSize),
		closed:           false,
	}, nil
}

func (t *RedisTransport) Dispatch(update *Update) error {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return ErrClosedTransport
	}

	AssignUUID(update)

	updateData, err := json.Marshal(*update)
	if err != nil {
		return fmt.Errorf("error when marshaling update: %w", err)
	}

	pipeline := t.client.TxPipeline()
	pipeline.XAdd(context.Background(), &redis.XAddArgs{Stream: t.stream, Values: map[string]interface{}{"updateID": update.ID, "updateData": updateData}})
	count := pipeline.Incr(context.Background(), t.stream+"-count")
	_, err = pipeline.Exec(context.Background())
	if err != nil {
		return fmt.Errorf("error when adding to Redis stream: %w", err)
	}

	needCleanup := t.historyLimit != 0 &&
		t.cleanupFrequency != 0 &&
		t.historyLimit < int(count.Val()) &&
		(t.cleanupFrequency == 1 || rand.Float64() >= t.cleanupFrequency)

	if needCleanup {
		trimmed, err := t.client.XTrimMaxLen(context.Background(), t.stream, int64(t.historyLimit)).Result()
		if err != nil {
			return fmt.Errorf("unable to cleanup Redis: %w", err)
		}
		_, err = t.client.DecrBy(context.Background(), t.stream+"-count", trimmed).Result()
		if err != nil {
			return fmt.Errorf("unable to cleanup Redis: %w", err)
		}
	}

	for _, s := range t.subscribers.MatchAny(update) {
		s.Dispatch(update, false)
	}

	return nil
}

func (t *RedisTransport) AddSubscriber(s *LocalSubscriber) error {
	t.Lock()
	if t.closed {
		t.Unlock()
		return ErrClosedTransport
	}
	t.subscribers.Add(s)
	t.Unlock()
	if s.RequestLastEventID != "" {
		readResponse, err := t.client.XRead(context.Background(), &redis.XReadArgs{Streams: []string{t.stream, s.RequestLastEventID}, Count: int64(t.streamBatch), Block: 0}).Result()
		if err != nil {
			return fmt.Errorf("unable to get history from Redis: %w", err)
		}
		for _, message := range readResponse {
			for _, msg := range message.Messages {
				var update *Update
				if err := json.Unmarshal([]byte(msg.Values["updateData"].(string)), &update); err != nil {
					return fmt.Errorf("unable to unmarshal update: %w", err)
				}
				if !s.Dispatch(update, true) {
					break
				}
			}
		}
	}
	s.Ready()
	return nil
}

func (t *RedisTransport) RemoveSubscriber(s *LocalSubscriber) error {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return ErrClosedTransport
	}
	t.subscribers.Remove(s)
	return nil
}

func (t *RedisTransport) GetSubscribers() (string, []*Subscriber, error) {
	t.RLock()
	defer t.RUnlock()

	lastEventId := EarliestLastEventID
	readResponse, err := t.client.XRead(context.Background(), &redis.XReadArgs{Streams: []string{t.stream, ">"}, Count: 1, Block: 0}).Result()
	if err != nil {
		return "", nil, &TransportError{err: err}
	}

	if len(readResponse) > 0 && len(readResponse[0].Messages) > 0 {
		lastEventId = readResponse[0].Messages[0].ID
	}

	return lastEventId, getSubscribers(t.subscribers), nil
}

func (t *RedisTransport) Close() (err error) {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return ErrClosedTransport
	}
	t.subscribers.Walk(0, func(s *LocalSubscriber) bool {
		s.Disconnect()
		return true
	})
	err = t.client.Close()
	t.closed = true
	return nil
}

var (
	_ Transport            = (*RedisTransport)(nil)
	_ TransportSubscribers = (*RedisTransport)(nil)
)
