package main

import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"github.com/uglyer/ha-sqlite/node"
	"github.com/uglyer/ha-sqlite/tool"
	"log"
	"net"
	"path"
)

type Config struct {
	HaSqlite node.HaSqliteConfig `yaml:"ha_sqlite"`
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
	// 设置配置文件名，没有后缀
	dir := path.Dir(configFile)
	filename := path.Base(configFile)
	log.Printf("config file:%s", configFile)
	viperConfig.SetConfigFile(configFile)
	viperConfig.AddConfigPath(".")
	if !tool.FSPathIsExist(configFile) {
		// 设置默认值
		viperConfig.SetDefault("ha_sqlite", map[string]interface{}{
			"address":        "localhost:30051",
			"raft_id":        "",
			"data_path":      "data/",
			"raft_bootstrap": false,
			"raft_admin":     false,
			"join_address":   "",
		})
		if autoGenerateFile {
			err := viperConfig.WriteConfigAs(path.Join(dir, filename))
			if err != nil {
				return nil, fmt.Errorf("自动生成配置文件失败！%v\n", err)
			}
		}
	}
	// 读取解析
	if err := viperConfig.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("配置文件未找到或解析失败！%v\n", err)
	}
	log.Printf("node:%s", viperConfig.GetString("ha_sqlite.raft_id"))
	// 映射到结构体
	var config Config
	// TODO 配置映射解析失败
	if err := viperConfig.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("配置映射错误,%v\n", err)
	}
	if config.HaSqlite.RaftId == "" {
		return nil, fmt.Errorf("yaml raft_id is required")
	}
	if config.HaSqlite.RaftBootstrap && config.HaSqlite.JoinAddress != "" {
		return nil, fmt.Errorf("yaml raft_bootstrap 与 join_address 为互斥项")
	}
	_, port, err := net.SplitHostPort(config.HaSqlite.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse local address (%q): %v", config.HaSqlite.Address, err)
	}
	config.HaSqlite.LocalPort = port
	return &config, nil
}
