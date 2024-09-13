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

// NATSConfig ...
type NATSConfig struct {
	Brokers  []string `mapstructure:"brokers" json:"brokers"`
	Subjects []string `mapstructure:"subjects" json:"subjects"`

	// NATS JetStream stream name
	StreamName string `mapstructure:"stream_name" json:"strean_name"`

	// Jetstream consumer name (must be unique for every NATS consumer)
	ConsumerName string `mapstructure:"consumer_name" json:"consumer_name"`

	// MaxPollRecords, MaxPollBytes - currently not in use (reserved for future)
	// MaxPollRecords
	MaxPollRecords int `mapstructure:"max_poll_records" json:"max_poll_records"`
	// MaxPollBytes
	MaxPollBytes int `mapstructure:"max_poll_bytes" json:"max_poll_bytes"`

	// RetryOnFailedConnect
	RetryOnFailedConnect bool `mapstructure:"retry_on_failed_connect" json:"retry_on_failed_connect"`

	// CreateStreamIfNotExist
	CreateStreamIfNotExist bool `mapstructure:"create_stream_if_not_exist" json:"create_stream_if_not_exist"`

	// TLS may be enabled, and mTLS auth may be configured.
	TLS              bool `mapstructure:"tls" json:"tls"`
	tools.TLSOptions `mapstructure:",squash"`
}

// NATSConsumer ...
type NATSConsumer struct {
	name       string
	config     NATSConfig
	nc         *nats.Conn
	stream     jetstream.Stream
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
		return nil, errors.New("topics required")
	}
	if len(config.ConsumerName) == 0 {
		return nil, errors.New("consumer_name required")
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
		nats.RetryOnFailedConnect(config.RetryOnFailedConnect),  // reconnect
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
	consumers := c.stream.ListConsumers(context.Background())

	for cons := range consumers.Info() {
		if err := c.stream.DeleteConsumer(context.Background(), cons.Name); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS delete consumer error on shutdown", map[string]any{"error": err.Error(), "consumer_name": cons.Name}))
		} else {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "NATS consumer deleted", map[string]any{"consumer_name": cons.Name}))
		}
	}
	if err := c.nc.Drain(); err != nil {
		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS connection drain error on shutdown", map[string]any{"error": err.Error()}))
	}
}

func (c *NATSConsumer) connectedHandler() nats.ConnHandler {
	return func(conn *nats.Conn) {
		js, err := jetstream.New(conn)
		if err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS connect error", map[string]any{"error": err}))
			return
		}

		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "NATS consumer connected", map[string]any{"consumer_name": c.name}))

		c.stream, err = js.Stream(context.Background(), c.config.StreamName)
		if err != nil {
			if errors.Is(err, jetstream.ErrStreamNotFound) && c.config.CreateStreamIfNotExist {
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "NATS create new stream"))

				c.stream, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
					Name:      c.config.StreamName,
					Subjects:  c.config.Subjects,
					Retention: jetstream.WorkQueuePolicy,
				})
				if err != nil {
					c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS stream create error", map[string]any{"message": err}))
					return
				}
			} else {
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS stream open error", map[string]any{"message": err}))
				return
			}
		}

		jsConsumer, err := c.stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
			Name:           c.config.ConsumerName,
			FilterSubjects: c.config.Subjects,
			// MaxRequestBatch:    c.config.MaxPollRecords,
			// MaxRequestMaxBytes: c.config.MaxPollBytes,
		})
		if err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS create consumer error", map[string]any{"message": err}))
			return
		}

		if _, err := jsConsumer.Consume(
			c.messageHandler(context.Background()),
			jetstream.ConsumeErrHandler(c.errorHandler()),
			jetstream.PullHeartbeat(5*time.Second),
		); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS consume error", map[string]any{"message": err}))
		}

	}
}

func (c *NATSConsumer) messageHandler(ctx context.Context) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		defer func() {
			if err := msg.Ack(); err != nil {
				c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "NATS acknowledge message error", map[string]any{"error": err.Error()}))
			}
		}()

		var ev NATSMessage

		if err := json.Unmarshal(msg.Data(), &ev); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error unmarshalling event from NATS", map[string]any{"error": err.Error(), "subject": msg.Subject()}))
		}

		if err := c.dispatcher.Dispatch(ctx, ev.Method, ev.Payload); err != nil {
			c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error processing consumed event", map[string]any{"error": err.Error(), "method": ev.Method}))
		}
	}
}

func (c *NATSConsumer) reconnectHandler() nats.ConnHandler {
	return func(conn *nats.Conn) {
		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelWarn, "NATS consumer reconnected", map[string]any{"consumer_name": c.name}))
	}
}

func (c *NATSConsumer) disconnectHandler() nats.ConnHandler {
	return func(conn *nats.Conn) {
		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "NATS consumer disconnected", map[string]any{"consumer_name": c.name}))
	}
}

func (c *NATSConsumer) errorHandler() jetstream.ConsumeErrHandlerFunc {
	return func(consumeCtx jetstream.ConsumeContext, err error) {
		c.logger.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "NATS consumer error", map[string]any{"error": err}))
	}
}
