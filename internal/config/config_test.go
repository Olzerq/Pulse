package config

import (
	"testing"
	"time"
)

func TestTelegramConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*Config)
		wantError bool
		wantOn    bool
	}{
		{name: "disabled"},
		{
			name: "enabled",
			configure: func(cfg *Config) {
				cfg.TelegramBotToken = "token"
				cfg.TelegramChatID = "12345"
			},
			wantOn: true,
		},
		{
			name: "token without chat",
			configure: func(cfg *Config) {
				cfg.TelegramBotToken = "token"
			},
			wantError: true,
		},
		{
			name: "invalid API URL",
			configure: func(cfg *Config) {
				cfg.TelegramAPIURL = "not-a-url"
			},
			wantError: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := validConfig()
			if test.configure != nil {
				test.configure(&cfg)
			}
			err := cfg.Validate()
			if (err != nil) != test.wantError {
				t.Fatalf("Validate() error = %v, wantError %v", err, test.wantError)
			}
			if cfg.TelegramEnabled() != test.wantOn {
				t.Errorf("TelegramEnabled() = %v, want %v", cfg.TelegramEnabled(), test.wantOn)
			}
		})
	}
}

func TestPingerLockGraceMustBePositive(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.PingerLockGrace = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want Pinger lock grace validation error")
	}
}

func validConfig() Config {
	return Config{
		Service:                "test",
		Environment:            "test",
		LogLevel:               "info",
		HTTPAddr:               ":8080",
		ShutdownTimeout:        time.Second,
		PostgresURL:            "postgres://localhost/test",
		RedisAddr:              "localhost:6379",
		RedisOperationTimeout:  time.Second,
		KafkaBrokers:           []string{"localhost:9092"},
		CheckResultTopic:       "check.result",
		KafkaPublishTimeout:    time.Second,
		KafkaConsumerGroup:     "test-consumer",
		ConsumerProcessTimeout: time.Second,
		PingerPoll:             time.Second,
		PingerWorkers:          1,
		PingerUserAgent:        "Pulse/Test",
		PingerLockGrace:        time.Second,
		TelegramAPIURL:         "https://api.telegram.org",
		TelegramRequestTimeout: time.Second,
	}
}
