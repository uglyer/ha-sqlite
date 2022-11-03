package log

import "path"

// LogConfig 日志参数
type LogConfig struct {
	Path         string        `mapstructure:"path" yaml:"path"`
	AccessOption *RotateOption `mapstructure:"access-option" yaml:"access-option"`
	ErrorOption  *RotateOption `mapstructure:"error-option" yaml:"error-option"`
}

// SetDefaultLogMode 设置默认大日志模式
func SetDefaultLogMode(config *LogConfig) {
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
