package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type ChaosConfig struct {
    Enabled     bool    `yaml:"enabled"`
    FailureRate float32 `yaml:"failure_rate"`
}

type Config struct {
	Server  ServerConfig `yaml:"server"`
	Nodes   []string     `yaml:"nodes"`
	Logging string       `yaml:"logging"`
	Chaos   ChaosConfig  `yaml:"chaos"`
}

type ServerConfig struct {
	Port   string `yaml:"port"`
	APIKey string `yaml:"api_key"`
}

func LoadConfig(file, port string) (*Config, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	cfg := &Config{}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	if port != "" {
		cfg.Server.Port = port
	}

	cfg.Logging = strings.ReplaceAll(cfg.Logging, "{port}", cfg.Server.Port)
	return cfg, nil
}
