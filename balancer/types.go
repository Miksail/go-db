package balancer

import (
	"github.com/jackc/pgx/v4"
)

type Option func(*pgx.TxOptions)

func WithReadOnly() Option {
	return func(options *pgx.TxOptions) {
		options.AccessMode = pgx.ReadOnly
	}
}

type contextKey int

const (
	txContextKey contextKey = iota
)
