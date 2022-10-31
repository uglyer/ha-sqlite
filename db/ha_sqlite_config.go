package db

type HaSqliteConfig struct {
	// Address TCP host+port for this node
	Address string `mapstructure:"address" yaml:"address"`
	// DataPath is path to node data. Always set.
	DataPath string `mapstructure:"data-path" yaml:"data-path"`
	// ManagerDBPath is path to manager db file. Always set.
	ManagerDBPath string `mapstructure:"manager-db-path" yaml:"manager-db-path"`
}
