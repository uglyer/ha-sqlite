package proto

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	gProto "google.golang.org/protobuf/proto"
	"log"
	"math/rand"
	"strings"
	"time"
)

// ParametersToValues maps values in the proto params to SQL driver values.
func ParametersToValues(parameters []*Parameter) ([]interface{}, error) {
	if parameters == nil {
		return nil, nil
	}

	values := make([]interface{}, len(parameters))
	for i := range parameters {
		switch w := parameters[i].GetValue().(type) {
		case *Parameter_I:
			values[i] = sql.Named(parameters[i].GetName(), w.I)
		case *Parameter_D:
			values[i] = sql.Named(parameters[i].GetName(), w.D)
		case *Parameter_B:
			values[i] = sql.Named(parameters[i].GetName(), w.B)
		case *Parameter_Y:
			values[i] = sql.Named(parameters[i].GetName(), w.Y)
		case *Parameter_S:
			values[i] = sql.Named(parameters[i].GetName(), w.S)
		default:
			return nil, fmt.Errorf("unsupported type: %T", w)
		}
	}
	return values, nil
}

// ParametersCopyToDriverValues maps values in the proto params to SQL driver values.
func ParametersCopyToDriverValues(values []driver.Value, parameters []*Parameter) error {
	if parameters == nil {
		return nil
	}

	for i := range parameters {
		switch w := parameters[i].GetValue().(type) {
		case *Parameter_I:
			values[i] = w.I
		case *Parameter_D:
			values[i] = w.D
		case *Parameter_B:
			values[i] = w.B
		case *Parameter_Y:
			values[i] = w.Y
		case *Parameter_S:
			values[i] = w.S
		default:
			return fmt.Errorf("unsupported type: %T", w)
		}
	}
	return nil
}

// NormalizeRowValues performs some normalization of values in the returned rows.
// Text values come over (from sqlite-go) as []byte instead of strings
// for some reason, so we have explicitly converted (but only when type
// is "text" so we don't affect BLOB types)
func NormalizeRowValues(row []interface{}, types []string) ([]*Parameter, error) {
	values := make([]*Parameter, len(types))
	for i, v := range row {
		switch val := v.(type) {
		case int:
			values[i] = &Parameter{
				Value: &Parameter_I{
					I: int64(val),
				},
			}
		case int64:
			values[i] = &Parameter{
				Value: &Parameter_I{
					I: val,
				},
			}
		case float32:
			values[i] = &Parameter{
				Value: &Parameter_D{
					D: float64(val),
				},
			}
		case float64:
			values[i] = &Parameter{
				Value: &Parameter_D{
					D: val,
				},
			}
		case bool:
			values[i] = &Parameter{
				Value: &Parameter_B{
					B: val,
				},
			}
		case string:
			values[i] = &Parameter{
				Value: &Parameter_S{
					S: val,
				},
			}
		case []byte:
			if isTextType(types[i]) {
				values[i].Value = &Parameter_S{
					S: string(val),
				}
			} else {
				values[i] = &Parameter{
					Value: &Parameter_Y{
						Y: val,
					},
				}
			}
		case time.Time:
			rfc3339, err := val.MarshalText()
			if err != nil {
				return nil, err
			}
			values[i] = &Parameter{
				Value: &Parameter_S{
					S: string(rfc3339),
				},
			}
		case nil:
			continue
		default:
			return nil, fmt.Errorf("unhandled column type: %T %v", val, val)
		}
	}
	return values, nil
}

// isTextType returns whether the given type has a SQLite text affinity.
// http://www.sqlite.org/datatype3.html
func isTextType(t string) bool {
	return t == "text" ||
		t == "json" ||
		t == "" ||
		strings.HasPrefix(t, "varchar") ||
		strings.HasPrefix(t, "varying character") ||
		strings.HasPrefix(t, "nchar") ||
		strings.HasPrefix(t, "native character") ||
		strings.HasPrefix(t, "nvarchar") ||
		strings.HasPrefix(t, "clob")
}

func randomString() string {
	var output strings.Builder
	chars := "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP"

	for i := 0; i < 20; i++ {
		random := rand.Intn(len(chars))
		randomChar := chars[random]
		output.WriteString(string(randomChar))
	}
	return output.String()
}

// Convert a driver.Value slice into a driver.NamedValue slice.
func ValuesToNamedValues(args []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(args))
	for i, value := range args {
		namedValues[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   value,
		}
	}
	return namedValues
}

func DriverNamedValueToParameters(args []driver.NamedValue) ([]*Parameter, error) {
	parameter := make([]*Parameter, len(args))
	for i, value := range args {
		switch val := value.Value.(type) {
		case int:
			parameter[i] = &Parameter{
				Name: value.Name,
				Value: &Parameter_I{
					I: int64(val),
				},
			}
		case int64:
			parameter[i] = &Parameter{
				Name: value.Name,
				Value: &Parameter_I{
					I: val,
				},
			}
		case float32:
			parameter[i] = &Parameter{
				Name: value.Name,
				Value: &Parameter_D{
					D: float64(val),
				},
			}
		case float64:
			parameter[i] = &Parameter{
				Name: value.Name,
				Value: &Parameter_D{
					D: val,
				},
			}
		case bool:
			parameter[i] = &Parameter{
				Name: value.Name,
				Value: &Parameter_B{
					B: val,
				},
			}
		case string:
			parameter[i] = &Parameter{
				Name: value.Name,
				Value: &Parameter_S{
					S: val,
				},
			}
		case []byte:
			parameter[i] = &Parameter{
				Name: value.Name,
				Value: &Parameter_Y{
					Y: val,
				},
			}
		case time.Time:
			rfc3339, err := val.MarshalText()
			if err != nil {
				return nil, err
			}
			parameter[i] = &Parameter{
				Name: value.Name,
				Value: &Parameter_S{
					S: string(rfc3339),
				},
			}
		case nil:
			log.Printf("nil:%v", value.Value)
			continue
		default:
			log.Printf("float64:%v", value.Value)
			return nil, fmt.Errorf("unhandled column type: %T %v", val, val)
		}
	}
	return parameter, nil
}

func ValuesToParameters(args []driver.Value) ([]*Parameter, error) {
	return DriverNamedValueToParameters(ValuesToNamedValues(args))
}

func (b *BeginTxRequest) TxOptions() *sql.TxOptions {
	return &sql.TxOptions{
		Isolation: b.IsolationLevel(),
		ReadOnly:  b.Readonly,
	}
}
func (b *BeginTxRequest) IsolationLevel() sql.IsolationLevel {
	if b.Type == BeginTxRequest_TX_TYPE_BEGIN_LevelDefault {
		return sql.LevelDefault
	} else if b.Type == BeginTxRequest_TX_TYPE_BEGIN_LevelReadUncommitted {
		return sql.LevelReadUncommitted
	} else if b.Type == BeginTxRequest_TX_TYPE_BEGIN_LevelReadCommitted {
		return sql.LevelReadCommitted
	} else if b.Type == BeginTxRequest_TX_TYPE_BEGIN_LevelWriteCommitted {
		return sql.LevelWriteCommitted
	} else if b.Type == BeginTxRequest_TX_TYPE_BEGIN_LevelRepeatableRead {
		return sql.LevelRepeatableRead
	} else if b.Type == BeginTxRequest_TX_TYPE_BEGIN_LevelSnapshot {
		return sql.LevelSnapshot
	} else if b.Type == BeginTxRequest_TX_TYPE_BEGIN_LevelSerializable {
		return sql.LevelSerializable
	} else if b.Type == BeginTxRequest_TX_TYPE_BEGIN_LevelLinearizable {
		return sql.LevelLinearizable
	}
	return sql.LevelDefault
}

func toCommandBytes(t Command_Type, req gProto.Message) ([]byte, error) {
	b, err := gProto.Marshal(req)
	if err != nil {
		return nil, err
	}
	command := &Command{
		Type:       t,
		SubCommand: b,
		Compressed: false,
	}
	b, err = gProto.Marshal(command)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func BytesToCommandBytes(t Command_Type, dbId int64, b []byte) ([]byte, error) {
	command := &Command{
		Type:       t,
		SubCommand: b,
		DbId:       dbId,
		Compressed: false,
	}
	b, err := gProto.Marshal(command)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (req *ExecRequest) ToCommandBytes() ([]byte, error) {
	return toCommandBytes(Command_COMMAND_TYPE_EXEC, req)
}

func (req *OpenRequest) ToCommandBytes() ([]byte, error) {
	return toCommandBytes(Command_COMMAND_TYPE_OPEN, req)
}

func (req *BeginTxRequest) ToCommandBytes() ([]byte, error) {
	return toCommandBytes(Command_COMMAND_TYPE_BEGIN_TX, req)
}

func (req *FinishTxRequest) ToCommandBytes() ([]byte, error) {
	return toCommandBytes(Command_COMMAND_TYPE_FINISH_TX, req)
}
