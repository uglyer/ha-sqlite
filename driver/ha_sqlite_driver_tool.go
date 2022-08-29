package driver

import (
	"database/sql/driver"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"time"
)

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

func DriverNamedValueToParameters(args []driver.NamedValue) ([]*proto.Parameter, error) {
	parameter := make([]*proto.Parameter, len(args))
	for i, value := range args {
		switch val := value.Value.(type) {
		case int:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_I{
					I: int64(val),
				},
			}
		case int64:
			log.Printf("int:%v", value.Value)
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_I{
					I: val,
				},
			}
		case float32:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_D{
					D: float64(val),
				},
			}
		case float64:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_D{
					D: val,
				},
			}
		case bool:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_B{
					B: val,
				},
			}
		case string:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_S{
					S: val,
				},
			}
		case []byte:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_Y{
					Y: val,
				},
			}
		case time.Time:
			rfc3339, err := val.MarshalText()
			if err != nil {
				return nil, err
			}
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_S{
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

func ValuesToParameters(args []driver.Value) ([]*proto.Parameter, error) {
	return DriverNamedValueToParameters(ValuesToNamedValues(args))
}
