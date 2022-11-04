package main

import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/log"
	"github.com/uglyer/ha-sqlite/tool"
)

type Config struct {
	HaSqlite *db.HaSqliteConfig `mapstructure:"ha-sqlite" yaml:"ha-sqlite"`
	Log      *log.LogConfig     `mapstructure:"log" yaml:"log"`
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
	return &config, nil
}
