package log

// SetDefaultLogMode 设置默认大日志模式
func SetDefaultLogMode() {
	var tops = []TeeOption{
		{
			Filename: "access.log",
			Ropt: RotateOption{
				MaxSize:    1,
				MaxAge:     1,
				MaxBackups: 3,
				Compress:   true,
			},
			Lef: func(lvl Level) bool {
				return lvl <= InfoLevel
			},
		},
		{
			Filename: "error.log",
			Ropt: RotateOption{
				MaxSize:    1,
				MaxAge:     1,
				MaxBackups: 3,
				Compress:   true,
			},
			Lef: func(lvl Level) bool {
				return lvl > InfoLevel
			},
		},
	}

	logger := NewTeeWithRotate(tops)
	ResetDefault(logger)
}
