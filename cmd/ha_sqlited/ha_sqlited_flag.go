package main

import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/log"
	"github.com/uglyer/ha-sqlite/s3"
	"github.com/uglyer/ha-sqlite/tool"
)

type Config struct {
	HaSqlite *db.HaSqliteConfig `mapstructure:"ha-sqlite" yaml:"ha-sqlite"`
	Log      *log.LogConfig     `mapstructure:"log" yaml:"log"`
	S3       *s3.S3Config       `mapstructure:"s3" yaml:"s3"`
}

// ParseFlags parses the command line, and returns the configuration.
func ParseFlags() (*Config, error) {
	if flag.Parsed() {
		return nil, fmt.Errorf("command-line flags already parsed")
	}
	var configFile string
	var autoGenerateFile bool
	flag.StringVar(&configFile, "config", "ha-sqlite.yaml", "config yaml file")
	flag.BoolVar(&autoGenerateFile, "auto-generate-config", false, "auto generate config yaml file")
	flag.Parse()
	viperConfig := viper.New()
	viperConfig.SetConfigFile(configFile)
	viperConfig.AddConfigPath(".")
	viperConfig.AutomaticEnv()
	// 设置默认值
	viperConfig.SetDefault("ha-sqlite", map[string]interface{}{
		"address":         "localhost:30051",
		"data-path":       "data/",
		"manager-db-path": "manager.db",
	})
	viperConfig.SetDefault("log", map[string]interface{}{
		"path":          "log",
		"console-level": "info",
		"access-option": map[string]interface{}{
			"level":       "info",
			"max-size":    1,
			"max-age":     1,
			"max-backups": 3,
			"compress":    true,
		},
		"error-option": map[string]interface{}{
			"level":       "warn",
			"max-size":    1,
			"max-age":     1,
			"max-backups": 3,
			"compress":    true,
		},
	})
	viperConfig.SetDefault("s3", map[string]interface{}{
		"enabled":     false,
		"access-key":  "",
		"secret-key":  "",
		"endpoint":    "",
		"region":      "",
		"disable-ssl": false,
		"bucket":      "",
		"prefix-path": "",
	})
	if !tool.FSPathIsExist(configFile) {
		if autoGenerateFile {
			err := viperConfig.WriteConfig()
			if err != nil {
				return nil, fmt.Errorf("自动生成配置文件失败！%v\n", err)
			}
		}
	}
	// 读取解析
	if err := viperConfig.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("配置文件未找到或解析失败！%v\n", err)
	}
	// 映射到结构体
	var config Config
	if err := viperConfig.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("配置映射错误,%v\n", err)
	}
	if config.HaSqlite.DataPath == "" {
		return nil, fmt.Errorf("yaml data-path is required")
	}
	if config.S3.Enabled {
		if config.S3.AccessKey == "" {
			return nil, fmt.Errorf("yaml s3:AccessKey is empty")
		}
		if config.S3.SecretKey == "" {
			return nil, fmt.Errorf("yaml s3:SecretKey is empty")
		}
		if config.S3.Endpoint == "" {
			return nil, fmt.Errorf("yaml s3:Endpoint is empty")
		}
		if config.S3.Bucket == "" {
			return nil, fmt.Errorf("yaml s3:Bucket is empty")
		}
	}
	return &config, nil
}
