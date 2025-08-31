package examples

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Miksail/go-db/balancer"
)

// NewTransactionalBalancer create transactional balancer and run goose migration
func NewTransactionalBalancer(ctx context.Context, dsn string) (balancer.TransactionalBalancer, func(), error) {
	// create pgx connection
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		return nil, nil, err
	}
	return balancer.NewBalancer(pool), pool.Close, nil
}
