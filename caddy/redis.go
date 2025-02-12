package caddy

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
)

func init() {
	caddy.RegisterModule(Redis{})
}

type Redis struct {
	Address                      string
	Username                     string
	Password                     string
	SubscribersSize              int
	SubscribersBroadcastParallel int
	ReceiveTimer                 time.Duration

	transport    *mercure.RedisTransport
	transportKey string
}

func (Redis) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mercure.redis",
		New: func() caddy.Module { return new(Redis) },
	}
}

func (r *Redis) GetTransport() mercure.Transport {
	return r.transport
}

func (r *Redis) Provision(ctx caddy.Context) error {
	var key bytes.Buffer
	if err := gob.NewEncoder(&key).Encode(r); err != nil {
		return err
	}
	r.transportKey = key.String()

	destructor, _, err := TransportUsagePool.LoadOrNew(r.transportKey, func() (caddy.Destructor, error) {
		t, err := mercure.NewRedisTransport(ctx.Logger(), r.Address, r.Username, r.Password, r.ReceiveTimer, r.SubscribersSize, r.SubscribersBroadcastParallel)
		if err != nil {
			return nil, err
		}

		return TransportDestructor[*mercure.RedisTransport]{Transport: t.(*mercure.RedisTransport)}, nil
	})
	if err != nil {
		return err
	}

	r.transport = destructor.(TransportDestructor[*mercure.RedisTransport]).Transport

	return nil
}

func (r *Redis) Cleanup() error {
	_, err := TransportUsagePool.Delete(r.transportKey)

	return err
}

func (r *Redis) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "address":
				if !d.NextArg() {
					return d.ArgErr()
				}

				r.Address = d.Val()

			case "username":
				if !d.NextArg() {
					return d.ArgErr()
				}

				r.Username = d.Val()

			case "password":
				if !d.NextArg() {
					return d.ArgErr()
				}

				r.Username = d.Val()

			case "receive_timer":
				if !d.NextArg() {
					return d.ArgErr()
				}

				receive_timer, err := time.ParseDuration(d.Val())
				if err != nil {
					return err
				}
				r.ReceiveTimer = receive_timer

			case "subscribers_size":
				if !d.NextArg() {
					return d.ArgErr()
				}

				s, e := strconv.Atoi(d.Val())
				if e != nil {
					return e
				}

				r.SubscribersSize = s

			case "subscribers_broadcast_parallel":
				if !d.NextArg() {
					return d.ArgErr()
				}

				s, e := strconv.Atoi(d.Val())
				if e != nil {
					return e
				}

				r.SubscribersBroadcastParallel = s
			}
		}
	}

	return nil
}

var (
	_ caddy.Provisioner     = (*Redis)(nil)
	_ caddy.CleanerUpper    = (*Redis)(nil)
	_ caddyfile.Unmarshaler = (*Redis)(nil)
)
