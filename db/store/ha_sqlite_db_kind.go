package store

var kindMap map[string]string

func init() {
	kindMap = map[string]string{
		"bool":    "real",
		"int":     "integer",
		"int8":    "integer",
		"int16":   "integer",
		"int32":   "integer",
		"int64":   "integer",
		"uint":    "integer",
		"uint8":   "integer",
		"uint16":  "integer",
		"uint32":  "integer",
		"uint64":  "integer",
		"float32": "number",
		"float64": "number",
		"string":  "text",
	}
}

func getSqliteTypeFromKindName(kind string) string {
	typeName, ok := kindMap[kind]
	if !ok {
		return ""
	}
	return typeName
}
