package mercure

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

const (
	defaultRedisStreamName       = "updates"
	defaultRedisSubscribersSize  = 100000
	defaultRedisHistoryLimit     = 0
	defaultRedisCleanupFrequency = 0.3
)

const (
	dispatchScript = `
	local stream = KEYS[1]
	local updateID = ARGV[1]
	local updateData = ARGV[2]
	local historyLimit = tonumber(ARGV[3])
	local cleanupFrequency = tonumber(ARGV[4])
	
	redis.call("XADD", stream, "*", 
			"updateID", updateID,
			"updateData", updateData
	)
	
	local length = redis.call("XLEN", stream)
	
	if historyLimit > 0 and cleanupFrequency > 0 and length > historyLimit then
			if cleanupFrequency == 1 or math.random() < (1 / cleanupFrequency) then
					redis.call("XTRIM", stream, "MAXLEN", historyLimit)
			end
	end
	
	return true
`
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
	historyLimit     int
	subscribersSize  int
	cleanupFrequency float64
	closed           bool
	dispatch         *redis.Script
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

	return NewRedisTransportInstance(logger, client, streamName, historyLimit, subscribersSize, cleanupFrequency)
}

func NewRedisTransportInstance(
	logger Logger,
	client *redis.Client,
	stream string,
	historyLimit int,
	subscribersSize int,
	cleanupFrequency float64,
) (*RedisTransport, error) {
	return &RedisTransport{
		logger:           logger,
		client:           client,
		stream:           stream,
		historyLimit:     historyLimit,
		cleanupFrequency: cleanupFrequency,
		subscribersSize:  subscribersSize,
		subscribers:      NewSubscriberList(subscribersSize),
		closed:           false,
		dispatch:         redis.NewScript(dispatchScript),
	}, nil
}

func (t *RedisTransport) Dispatch(update *Update) error {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return ErrClosedTransport
	}

	AssignUUID(update)

	updateData, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("error when marshaling update: %w", err)
	}

	_, err = t.dispatch.Run(context.Background(), t.client, []string{t.stream}, update.ID, updateData, t.historyLimit, t.cleanupFrequency).Result()
	if err != nil {
		return fmt.Errorf("redis script execution failed: %w", err)
	}

	for _, s := range t.subscribers.MatchAny(update) {
		s.Dispatch(update, false)
	}

	return nil
}

func (t *RedisTransport) AddSubscriber(s *LocalSubscriber) error {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return ErrClosedTransport
	}

	if s.RequestLastEventID != "" {
		messages, err := t.client.XRange(context.Background(), t.stream, s.RequestLastEventID, "+").Result()
		if err != nil {
			return fmt.Errorf("unable to get history from Redis: %w", err)
		}
		for _, msg := range messages {
			var update *Update
			if err := json.Unmarshal([]byte(msg.Values["updateData"].(string)), &update); err != nil {
				return fmt.Errorf("unable to unmarshal update: %w", err)
			}
			if !s.Dispatch(update, true) {
				break
			}
		}
	}

	t.subscribers.Add(s)
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
