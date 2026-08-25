package store

// ValueType identifies which Go-side field of Value is meaningful. A tagged
// union (Go has no native sum type) is used here instead of, say,
// `interface{}` per key, because it keeps type checking explicit and cheap:
// commands can switch on Type and return a clear "WRONGTYPE" style error
// instead of relying on a runtime type assertion that might panic.
type ValueType int

const (
	TypeString ValueType = iota
	TypeList
)

func (t ValueType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeList:
		return "list"
	default:
		return "unknown"
	}
}

// Value is the container stored for every key in the table. Only the field
// matching Type is populated; the others are left at their zero value.
type Value struct {
	Type ValueType
	Str  string
	List []string
}
