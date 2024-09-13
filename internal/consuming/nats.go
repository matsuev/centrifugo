package consuming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/centrifugal/centrifugo/v5/internal/tools"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/centrifugal/centrifuge"
)

// MapAny ...
type MapAny map[string]any

// NATSConfig ...
type NATSConfig struct {
	Brokers  []string `mapstructure:"brokers" json:"brokers"`
	Subjects []string `mapstructure:"subjects" json:"subjects"`

	// NATS JetStream stream name
	StreamName string `mapstructure:"stream_name" json:"stream_name"`

	// Jetstream consumer group name
	ConsumerGroup string `mapstructure:"consumer_group" json:"consumer_group"`

	// MaxPollRecords
	MaxPollRecords int `mapstructure:"max_poll_records" json:"max_poll_records"`

	// RetryOnFailedConnect
	RetryOnFailedConnect bool `mapstructure:"retry_on_failed_connect" json:"retry_on_failed_connect"`

	// CreateStreamIfNotExist
	CreateStreamIfNotExist bool `mapstructure:"create_stream_if_not_exist" json:"create_stream_if_not_exist"`

	// HeartbeatInterval
	HeartbeatInterval string `mapstructure:"heartbeat_interval" json:"heartbeat_interval"`

	// TLS may be enabled, and mTLS auth may be configured.
	TLS              bool `mapstructure:"tls" json:"tls"`
	tools.TLSOptions `mapstructure:",squash"`
}

const (
	DefaultHeartbitInterval = 5 * time.Second
	DefaultHeartbitString   = "5s"

	DefaultMaxPollRecords = 100

	DefaultMaxAge = time.Minute
)

// NATSConsumer ...
type NATSConsumer struct {
	name       string
	config     NATSConfig
	nc         *nats.Conn
	js         jetstream.JetStream
	stream     jetstream.Stream
	cons       jetstream.Consumer
	logger     Logger
	dispatcher Dispatcher
}

// NATSMessage ...
type NATSMessage struct {
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload"`
}

func NewNATSConsumer(name string, logger Logger, dispatcher Dispatcher, config NATSConfig) (*NATSConsumer, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("brokers required")
	}
	if len(config.Subjects) == 0 {
		return nil, errors.New("subjects required")
	}
	if len(config.StreamName) == 0 {
		return nil, errors.New("stream_name required")
	}
	if len(config.ConsumerGroup) == 0 {
		return nil, errors.New("consumer_group required")
	}
	if _, err := time.ParseDuration(config.HeartbeatInterval); err != nil {
		logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS heartbeat_interval  error on shutdown", MapAny{"error": err.Error()}))
		config.HeartbeatInterval = DefaultHeartbitString
	}
	if len(config.HeartbeatInterval) == 0 {
		config.HeartbeatInterval = DefaultHeartbitString
	}
	if config.MaxPollRecords < 1 {
		config.MaxPollRecords = DefaultMaxPollRecords
	}

	newConsumer := &NATSConsumer{
		name:       name,
		logger:     logger,
		dispatcher: dispatcher,
		config:     config,
	}

	natsUrl := strings.Join(config.Brokers, ",")

	nc, err := nats.Connect(
		natsUrl,
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(config.RetryOnFailedConnect),  // auto reconnect to broker
		nats.ConnectHandler(newConsumer.connectedHandler()),     //
		nats.ReconnectHandler(newConsumer.reconnectHandler()),   //
		nats.DisconnectHandler(newConsumer.disconnectHandler()), //
	)
	if err != nil {
		return nil, fmt.Errorf("error init NATS client: %w", err)
	}

	newConsumer.nc = nc

	return newConsumer, nil
}

// Run ...
func (c *NATSConsumer) Run(ctx context.Context) error {
	defer c.Drain()

	<-ctx.Done()

	return ctx.Err()
}

// Drain ...
func (c *NATSConsumer) Drain() {
	if err := c.nc.Drain(); err != nil {
		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS connection drain error on shutdown", MapAny{"error": err.Error()}))
	}
}

func (c *NATSConsumer) connectedHandler() nats.ConnHandler {
	return func(conn *nats.Conn) {
		var err error

		if c.js, err = jetstream.New(conn); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS connect error (attempting to reconnect)", MapAny{"error": err.Error(), "consumer_name": c.name}))

			return
		}

		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "NATS consumer connected", MapAny{"consumer_name": c.name}))

		if c.stream, err = c.js.Stream(context.Background(), c.config.StreamName); err != nil {
			if errors.Is(err, jetstream.ErrStreamNotFound) && c.config.CreateStreamIfNotExist {
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "NATS create new stream", MapAny{"consumer_name": c.name, "stream_name": c.config.StreamName}))

				if c.stream, err = c.js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
					Name:      c.config.StreamName,
					Subjects:  c.config.Subjects,
					Retention: jetstream.WorkQueuePolicy,
					MaxAge:    DefaultMaxAge,
				}); err != nil {
					c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS stream create error", MapAny{"error": err.Error(), "consumer_name": c.name, "stream_name": c.config.StreamName}))

					return
				}
			} else {
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS stream open error", MapAny{"error": err.Error(), "consumer_name": c.name, "stream_name": c.config.StreamName}))

				return
			}
		}

		if c.cons, err = c.stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
			Durable:        c.config.ConsumerGroup,
			FilterSubjects: c.config.Subjects,
		}); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS create consumer error", MapAny{"error": err.Error(), "consumer_name": c.name}))

			return
		}

		heartbitInterval, err := time.ParseDuration(c.config.HeartbeatInterval)
		if err != nil {
			heartbitInterval = DefaultHeartbitInterval
		}

		if _, err := c.cons.Consume(
			c.messageHandler(context.Background()),
			jetstream.ConsumeErrHandler(c.errorHandler()),
			jetstream.PullHeartbeat(heartbitInterval),
			jetstream.PullMaxMessages(c.config.MaxPollRecords),
		); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS consumer consume error", MapAny{"error": err.Error(), "consumer_name": c.name}))
		}

		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "NATS consumer started", MapAny{"consumer_name": c.name}))
	}
}

func (c *NATSConsumer) messageHandler(ctx context.Context) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		defer func() {
			if err := msg.Ack(); err != nil {
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS acknowledge message error", MapAny{"error": err.Error(), "consumer_name": c.name}))
			}
		}()

		var ev NATSMessage

		if err := json.Unmarshal(msg.Data(), &ev); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS unmarshalling event error", MapAny{"error": err.Error(), "subject": msg.Subject()}))
		}

		if err := c.dispatcher.Dispatch(ctx, ev.Method, ev.Payload); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS processing consumed event error", MapAny{"error": err.Error(), "consumer_name": c.name, "method": ev.Method}))
		}
	}
}

func (c *NATSConsumer) reconnectHandler() nats.ConnHandler {
	return func(conn *nats.Conn) {
		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS consumer reconnected", MapAny{"consumer_name": c.name}))
	}
}

func (c *NATSConsumer) disconnectHandler() nats.ConnHandler {
	return func(conn *nats.Conn) {
		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "NATS consumer disconnected", MapAny{"consumer_name": c.name}))
	}
}

func (c *NATSConsumer) errorHandler() jetstream.ConsumeErrHandlerFunc {
	return func(consumeCtx jetstream.ConsumeContext, err error) {
		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "NATS consumer error", MapAny{"error": err, "consumer_name": c.name}))
	}
}
