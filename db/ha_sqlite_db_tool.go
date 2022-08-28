package db

import (
	"database/sql"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
	"math/rand"
	"strings"
	"time"
)

// parametersToValues maps values in the proto params to SQL driver values.
func parametersToValues(parameters []*proto.Parameter) ([]interface{}, error) {
	if parameters == nil {
		return nil, nil
	}

	values := make([]interface{}, len(parameters))
	for i := range parameters {
		switch w := parameters[i].GetValue().(type) {
		case *proto.Parameter_I:
			values[i] = sql.Named(parameters[i].GetName(), w.I)
		case *proto.Parameter_D:
			values[i] = sql.Named(parameters[i].GetName(), w.D)
		case *proto.Parameter_B:
			values[i] = sql.Named(parameters[i].GetName(), w.B)
		case *proto.Parameter_Y:
			values[i] = sql.Named(parameters[i].GetName(), w.Y)
		case *proto.Parameter_S:
			values[i] = sql.Named(parameters[i].GetName(), w.S)
		default:
			return nil, fmt.Errorf("unsupported type: %T", w)
		}
	}
	return values, nil
}

// normalizeRowValues performs some normalization of values in the returned rows.
// Text values come over (from sqlite-go) as []byte instead of strings
// for some reason, so we have explicitly converted (but only when type
// is "text" so we don't affect BLOB types)
func normalizeRowValues(row []interface{}, types []string) ([]*proto.Parameter, error) {
	values := make([]*proto.Parameter, len(types))
	for i, v := range row {
		switch val := v.(type) {
		case int:
			values[i] = &proto.Parameter{
				Value: &proto.Parameter_I{
					I: int64(val),
				},
			}
		case int64:
			values[i] = &proto.Parameter{
				Value: &proto.Parameter_I{
					I: val,
				},
			}
		case float32:
			values[i] = &proto.Parameter{
				Value: &proto.Parameter_D{
					D: float64(val),
				},
			}
		case float64:
			values[i] = &proto.Parameter{
				Value: &proto.Parameter_D{
					D: val,
				},
			}
		case bool:
			values[i] = &proto.Parameter{
				Value: &proto.Parameter_B{
					B: val,
				},
			}
		case string:
			values[i] = &proto.Parameter{
				Value: &proto.Parameter_S{
					S: val,
				},
			}
		case []byte:
			if isTextType(types[i]) {
				values[i].Value = &proto.Parameter_S{
					S: string(val),
				}
			} else {
				values[i] = &proto.Parameter{
					Value: &proto.Parameter_Y{
						Y: val,
					},
				}
			}
		case time.Time:
			rfc3339, err := val.MarshalText()
			if err != nil {
				return nil, err
			}
			values[i] = &proto.Parameter{
				Value: &proto.Parameter_S{
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
