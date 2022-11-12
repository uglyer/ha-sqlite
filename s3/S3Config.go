package s3

// S3Config S3 存储桶配置参数
type S3Config struct {
	AccessKey  string `mapstructure:"access-key" yaml:"access-key"`
	SecretKey  string `mapstructure:"secret-key" yaml:"secret-key"`
	Endpoint   string `mapstructure:"endpoint" yaml:"endpoint"`
	Region     string `mapstructure:"region" yaml:"region"`
	DisableSSL bool   `mapstructure:"disable-ssl" yaml:"disable-ssl"`
	Bucket     string `mapstructure:"bucket" yaml:"bucket"`
}
