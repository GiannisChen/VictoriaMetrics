package vmsql

type Statement interface {
	Type() string
}

type CreateStatement struct {
	Table       Table
	IfNotExists bool
}

func (stmt *CreateStatement) Type() string {
	return "CREATE"
}
