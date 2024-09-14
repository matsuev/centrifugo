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

	// DisableReconnect
	DisableReconnect bool `mapstructure:"disable_reconnect" json:"disable_reconnect"`

	// HeartbeatInterval
	HeartbeatInterval string `mapstructure:"heartbeat_interval" json:"heartbeat_interval"`

	// CreateStreamIfNotExist
	CreateStreamIfNotExist bool `mapstructure:"create_stream_if_not_exist" json:"create_stream_if_not_exist"`

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
	if len(config.HeartbeatInterval) == 0 {
		config.HeartbeatInterval = DefaultHeartbitString
	}
	if _, err := time.ParseDuration(config.HeartbeatInterval); err != nil {
		logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS wrong heartbeat_interval (default value is used)", MapAny{"consumer_name": name}))
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
		nats.MaxReconnects(-1), // always reconnect
		nats.RetryOnFailedConnect(!config.DisableReconnect),     // auto reconnect to broker
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

		// Create JetStream connection
		if c.js, err = jetstream.New(conn); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS connect error (attempting to reconnect)", MapAny{"error": err.Error(), "consumer_name": c.name}))

			return
		}

		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "NATS consumer connected", MapAny{"consumer_name": c.name}))

		// !!!EXPEREMENTAL!!!
		// If stream not found and config.CreateStreamIfNotExist is true
		// try to create new stream with default options
		//
		// In production mode, you first need to initialize the broker, then set up the Centrifugo configuration for connection
		if c.stream, err = c.js.Stream(context.Background(), c.config.StreamName); err != nil {
			if errors.Is(err, jetstream.ErrStreamNotFound) && c.config.CreateStreamIfNotExist {

				// If stream does not exist, trying to create it
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "NATS create new stream", MapAny{"consumer_name": c.name, "stream_name": c.config.StreamName}))

				if c.stream, err = c.js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
					Name:      c.config.StreamName,
					Subjects:  c.config.Subjects,
					Retention: jetstream.WorkQueuePolicy, // !IMPORTANT! This ensures that the message will be delivered only once.
					MaxAge:    DefaultMaxAge,             // Message lifetime in queue, default is 1 minute
				}); err != nil {
					c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS stream create error", MapAny{"error": err.Error(), "consumer_name": c.name, "stream_name": c.config.StreamName}))

					return
				}
			} else {
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS stream open error", MapAny{"error": err.Error(), "consumer_name": c.name, "stream_name": c.config.StreamName}))

				return
			}
		}

		// Create NATS queue consumer
		if c.cons, err = c.stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
			Durable:        c.config.ConsumerGroup,
			FilterSubjects: c.config.Subjects,
		}); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS create consumer error", MapAny{"error": err.Error(), "consumer_name": c.name}))

			return
		}

		// Parse config.HeartbeatInterval from string to time.Duration
		heartbitInterval, err := time.ParseDuration(c.config.HeartbeatInterval)
		if err != nil {
			heartbitInterval = DefaultHeartbitInterval
		}

		// Set consumer handler function
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
		// confirmation of message processing
		defer func() {
			if err := msg.Ack(); err != nil {
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS acknowledge message error", MapAny{"error": err.Error(), "consumer_name": c.name}))
			}
		}()

		var ev NATSMessage

		// Try unmarshal message data to NTASMessage
		if err := json.Unmarshal(msg.Data(), &ev); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS unmarshalling event error", MapAny{"error": err.Error(), "subject": msg.Subject()}))
			return
		}

		// Dispatch message
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
