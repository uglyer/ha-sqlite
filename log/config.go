package log

import (
	"log"
	"path"
)

// LogConfig 日志参数
type LogConfig struct {
	Path         string        `mapstructure:"path" yaml:"path"`
	ConsoleLevel string        `mapstructure:"console-level" yaml:"console-level"`
	AccessOption *RotateOption `mapstructure:"access-option" yaml:"access-option"`
	ErrorOption  *RotateOption `mapstructure:"error-option" yaml:"error-option"`
}

// SetDefaultLogMode 设置默认大日志模式
func SetDefaultLogMode(config *LogConfig) {
	var consoleLevel Level
	err := consoleLevel.Set(config.ConsoleLevel)
	if err != nil {
		log.Fatalf("consoleLevel set error:%v", err)
	}
	log.Printf("consoleLevel:%v", consoleLevel)
	var tops = []TeeOption{
		{
			Filename: path.Join(config.Path, "access.log"),
			Ropt:     *config.AccessOption,
			Lef: func(lvl Level) bool {
				return lvl <= InfoLevel
			},
		},
		{
			Filename: path.Join(config.Path, "error.log"),
			Ropt:     *config.ErrorOption,
			Lef: func(lvl Level) bool {
				return lvl > InfoLevel
			},
		},
	}

	logger := NewTeeWithRotate(tops)
	ResetDefault(logger)
}
