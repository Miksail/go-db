package balancer

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

type Sqlizer interface {
	ToSql() (sql string, args []interface{}, err error)
}

type ExecutorRunner interface {
	Execx(ctx context.Context, sqlizer Sqlizer) (int64, error)
	Getx(ctx context.Context, dest interface{}, sqlizer Sqlizer) error
	Selectx(ctx context.Context, dest interface{}, sqlizer Sqlizer) error
}

type TransactionalBalancer interface {
	BeginTransaction(ctx context.Context, opts ...Option) (context.Context, error)
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error
	GetRunner(ctx context.Context) ExecutorRunner
}

type Balancer struct {
	pool *pgxpool.Pool
}

func (b *Balancer) BeginTransaction(ctx context.Context, opts ...Option) (context.Context, error) {
	if _, ok := ctx.Value(txContextKey).(pgx.Tx); ok {
		return ctx, nil
	}
	defaultOpts := &pgx.TxOptions{
		AccessMode: pgx.ReadWrite,
	}
	for _, o := range opts {
		o(defaultOpts)
	}

	tx, err := b.pool.BeginTx(ctx, *defaultOpts)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, txContextKey, tx)
	return ctx, nil
}

func (b *Balancer) CommitTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value(txContextKey).(pgx.Tx); ok {
		return tx.Commit(ctx)
	}
	return errors.New("not found transaction context key")
}

func (b *Balancer) RollbackTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value(txContextKey).(pgx.Tx); ok {
		if err := tx.Rollback(ctx); err != pgx.ErrTxClosed {
			return err
		}
		return nil
	}
	return errors.New("not found transaction context key")
}

func (b *Balancer) GetRunner(ctx context.Context) ExecutorRunner {
	if tx, ok := ctx.Value(txContextKey).(pgx.Tx); ok {
		return &txWrapper{tx: tx}
	}
	return &poolWrapper{pool: b.pool}
}

func NewBalancer(pool *pgxpool.Pool) TransactionalBalancer {
	b := &Balancer{pool: pool}
	return b
}
