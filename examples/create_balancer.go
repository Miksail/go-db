package examples

import (
	"context"

	"github.com/Miksail/go-db/balancer"
	"github.com/jackc/pgx/v4/pgxpool"
)

// NewTransactionalBalancer create transactional balancer and run goose migration
func NewTransactionalBalancer(ctx context.Context, dsn string, runMigrations bool) (balancer.TransactionalBalancer, func(), error) {
	// create pgx connection
	pool, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		return nil, nil, err
	}
	return balancer.NewBalancer(pool), pool.Close, nil
}
