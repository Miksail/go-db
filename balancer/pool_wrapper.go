package balancer

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Miksail/go-db/pgxscan"
)

type poolWrapper struct {
	pool *pgxpool.Pool
}

func (p *poolWrapper) Execx(ctx context.Context, sqlizer Sqlizer) (int64, error) {
	sql, args, err := sqlizer.ToSql()
	if err != nil {
		return 0, err
	}
	commandTag, err := p.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return commandTag.RowsAffected(), nil
}

func (p *poolWrapper) Getx(ctx context.Context, dest interface{}, sqlizer Sqlizer) error {
	sql, args, err := sqlizer.ToSql()
	if err != nil {
		return err
	}
	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	if err = pgxscan.ScanOne(dest, rows); err != nil {
		return err
	}
	return rows.Err()
}

func (p *poolWrapper) Selectx(ctx context.Context, dest interface{}, sqlizer Sqlizer) error {
	sql, args, err := sqlizer.ToSql()
	if err != nil {
		return err
	}
	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	if err = pgxscan.ScanAll(dest, rows); err != nil {
		return err
	}

	return rows.Err()
}

var _ ExecutorRunner = &poolWrapper{}
