package mercure

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	uri                         = "localhost:6379"
	receiveTimer                = 300 * time.Millisecond
	subscriberSize              = 100000
	subscriberBroadcastParallel = 16
)

func initialize() *RedisTransport {
	transport, _ := NewRedisTransport(zap.NewNop(), uri, "", "", receiveTimer, subscriberSize, subscriberBroadcastParallel)
	return transport.(*RedisTransport)
}

func TestRedisWaitListen(t *testing.T) {
	transport := initialize()
	defer transport.Close()
	assert.Implements(t, (*Transport)(nil), transport)
	s := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	require.NoError(t, transport.AddSubscriber(s))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for range s.Receive() {
			t.Fail()
		}
		wg.Done()
	}()
	s.Disconnect()
	wg.Wait()
}

func TestRedisDispatch(t *testing.T) {
	transport := initialize()
	defer transport.Close()
	assert.Implements(t, (*Transport)(nil), transport)

	subscriber := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	subscriber.SetTopics([]string{"https://topics.local/topic", "https://topics.local/private"}, []string{"https://topics.local/private"})
	require.NoError(t, transport.AddSubscriber(subscriber))
	notSubscribed := &Update{Topics: []string{"not-subscribed"}}
	require.NoError(t, transport.Dispatch(notSubscribed))
	subscribedSkipped := &Update{Topics: []string{"https://topics.local/topic"}, Private: true}
	require.NoError(t, transport.Dispatch(subscribedSkipped))
	public := &Update{Topics: subscriber.SubscribedTopics}
	require.NoError(t, transport.Dispatch(public))
	assert.Equal(t, public, <-subscriber.Receive())
	private := &Update{Topics: subscriber.AllowedPrivateTopics, Private: true}
	require.NoError(t, transport.Dispatch(private))
	assert.Equal(t, private, <-subscriber.Receive())
}

func TestRedisClose(t *testing.T) {
	transport := initialize()
	require.NotNil(t, transport)
	defer transport.Close()
	assert.Implements(t, (*Transport)(nil), transport)
	subscriber := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	subscriber.SetTopics([]string{"https://topics.local/topic"}, nil)
	require.NoError(t, transport.AddSubscriber(subscriber))
	require.NoError(t, transport.Close())
	require.Error(t, transport.AddSubscriber(subscriber))
	assert.Equal(t, transport.Dispatch(&Update{Topics: subscriber.SubscribedTopics}), ErrClosedTransport)
	_, ok := <-subscriber.out
	assert.False(t, ok)
}
