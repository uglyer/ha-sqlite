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
		log.Printf("xxx:%v", value.Value)
		// TODO 类型匹配存在问题
		//switch val := value.Value.(type) {
		//case int:
		//case int64:
		//case uint:
		//case uint64:
		//case int8:
		//case int16:
		//	log.Printf("int")
		//	break
		//default:
		//	log.Printf("xxxxxxx:%v", val)
		//}
		switch val := value.Value.(type) {
		case int:
		case int64:
			log.Printf("int:%v", value.Value)
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_I{
					I: val,
				},
			}
			break
		case float32:
		case float64:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_D{
					D: val,
				},
			}
			break
		case bool:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_B{
					B: val,
				},
			}
			break
		case string:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_S{
					S: val,
				},
			}
			break
		case []byte:
			parameter[i] = &proto.Parameter{
				Name: value.Name,
				Value: &proto.Parameter_Y{
					Y: val,
				},
			}
			break
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
			break
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
