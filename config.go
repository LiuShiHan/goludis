package main

import (
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	Addr   string `yaml:"addr"`
	Shards int    `yaml:"shards"`
}

func ReadConfig(path string) (*Config, error) {
	buf, err := os.ReadFile(path) // ① 把文件读成 []byte
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(buf, &cfg); err != nil { // ② 再解析
		return nil, err
	}
	return &cfg, nil
}
