// Package config loads and validates configuration shared by Pulse services.
package config

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	defaultEnvironment            = "development"
	defaultLogLevel               = "info"
	defaultHTTPAddr               = ":8080"
	defaultShutdownTimeout        = 10 * time.Second
	defaultPostgresURL            = "postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable"
	defaultRedisAddr              = "localhost:6379"
	defaultKafkaBroker            = "localhost:9092"
	defaultKafkaTopic             = "check.result"
	defaultKafkaPublishTimeout    = 10 * time.Second
	defaultConsumerGroup          = "pulse-consumer"
	defaultConsumerProcessTimeout = 10 * time.Second
	defaultPingerPoll             = time.Second
	defaultPingerWorkers          = 20
	defaultPingerUserAgent        = "Pulse/0.1"
)

// Config is the process configuration shared by all Pulse binaries. Keeping
// infrastructure addresses here gives later stages one configuration contract.
type Config struct {
	Service                string
	Environment            string
	LogLevel               string
	HTTPAddr               string
	ShutdownTimeout        time.Duration
	PostgresURL            string
	RedisAddr              string
	KafkaBrokers           []string
	CheckResultTopic       string
	KafkaPublishTimeout    time.Duration
	KafkaConsumerGroup     string
	ConsumerProcessTimeout time.Duration
	PingerPoll             time.Duration
	PingerWorkers          int
	PingerUserAgent        string
}

// Load reads configuration from the environment and applies local-development
// defaults. Environment variable names deliberately use a PULSE_ prefix.
func Load(service string) (Config, error) {
	shutdownTimeout, err := durationFromEnv("PULSE_SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return Config{}, err
	}
	kafkaPublishTimeout, err := durationFromEnv("PULSE_KAFKA_PUBLISH_TIMEOUT", defaultKafkaPublishTimeout)
	if err != nil {
		return Config{}, err
	}
	consumerProcessTimeout, err := durationFromEnv("PULSE_CONSUMER_PROCESS_TIMEOUT", defaultConsumerProcessTimeout)
	if err != nil {
		return Config{}, err
	}
	pingerPoll, err := durationFromEnv("PULSE_PINGER_POLL_INTERVAL", defaultPingerPoll)
	if err != nil {
		return Config{}, err
	}
	pingerWorkers, err := intFromEnv("PULSE_PINGER_MAX_CONCURRENCY", defaultPingerWorkers)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Service:                strings.TrimSpace(service),
		Environment:            envOrDefault("PULSE_ENV", defaultEnvironment),
		LogLevel:               strings.ToLower(envOrDefault("PULSE_LOG_LEVEL", defaultLogLevel)),
		HTTPAddr:               envOrDefault("PULSE_HTTP_ADDR", defaultHTTPAddr),
		ShutdownTimeout:        shutdownTimeout,
		PostgresURL:            envOrDefault("PULSE_POSTGRES_URL", defaultPostgresURL),
		RedisAddr:              envOrDefault("PULSE_REDIS_ADDR", defaultRedisAddr),
		KafkaBrokers:           csvFromEnv("PULSE_KAFKA_BROKERS", defaultKafkaBroker),
		CheckResultTopic:       envOrDefault("PULSE_KAFKA_CHECK_RESULTS_TOPIC", defaultKafkaTopic),
		KafkaPublishTimeout:    kafkaPublishTimeout,
		KafkaConsumerGroup:     envOrDefault("PULSE_KAFKA_CONSUMER_GROUP", defaultConsumerGroup),
		ConsumerProcessTimeout: consumerProcessTimeout,
		PingerPoll:             pingerPoll,
		PingerWorkers:          pingerWorkers,
		PingerUserAgent:        envOrDefault("PULSE_PINGER_USER_AGENT", defaultPingerUserAgent),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate configuration: %w", err)
	}

	return cfg, nil
}

// Validate rejects configuration that would otherwise fail later or result in
// surprising service behaviour.
func (c Config) Validate() error {
	var errs []error

	if c.Service == "" {
		errs = append(errs, errors.New("service name is required"))
	}
	if !slices.Contains([]string{"debug", "info", "warn", "error"}, c.LogLevel) {
		errs = append(errs, fmt.Errorf("PULSE_LOG_LEVEL must be debug, info, warn, or error, got %q", c.LogLevel))
	}
	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("PULSE_HTTP_ADDR is required"))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("PULSE_SHUTDOWN_TIMEOUT must be greater than zero"))
	}
	if c.PostgresURL == "" {
		errs = append(errs, errors.New("PULSE_POSTGRES_URL is required"))
	}
	if c.RedisAddr == "" {
		errs = append(errs, errors.New("PULSE_REDIS_ADDR is required"))
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("PULSE_KAFKA_BROKERS must contain at least one broker"))
	}
	if c.CheckResultTopic == "" {
		errs = append(errs, errors.New("PULSE_KAFKA_CHECK_RESULTS_TOPIC is required"))
	}
	if c.KafkaPublishTimeout <= 0 {
		errs = append(errs, errors.New("PULSE_KAFKA_PUBLISH_TIMEOUT must be greater than zero"))
	}
	if c.KafkaConsumerGroup == "" {
		errs = append(errs, errors.New("PULSE_KAFKA_CONSUMER_GROUP is required"))
	}
	if c.ConsumerProcessTimeout <= 0 {
		errs = append(errs, errors.New("PULSE_CONSUMER_PROCESS_TIMEOUT must be greater than zero"))
	}
	if c.PingerPoll < 100*time.Millisecond {
		errs = append(errs, errors.New("PULSE_PINGER_POLL_INTERVAL must be at least 100ms"))
	}
	if c.PingerWorkers < 1 || c.PingerWorkers > 1000 {
		errs = append(errs, errors.New("PULSE_PINGER_MAX_CONCURRENCY must be between 1 and 1000"))
	}
	if c.PingerUserAgent == "" {
		errs = append(errs, errors.New("PULSE_PINGER_USER_AGENT is required"))
	}

	return errors.Join(errs...)
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func durationFromEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return duration, nil
}

func intFromEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	integer, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return integer, nil
}

func csvFromEnv(key, fallback string) []string {
	value := envOrDefault(key, fallback)
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			items = append(items, item)
		}
	}
	return items
}
