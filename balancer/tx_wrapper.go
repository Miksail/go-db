package balancer

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/Miksail/go-db/pgxscan"
)

type txWrapper struct {
	tx pgx.Tx
}

func (t *txWrapper) Execx(ctx context.Context, sqlizer Sqlizer) (int64, error) {
	sql, args, err := sqlizer.ToSql()
	if err != nil {
		return 0, err
	}
	commandTag, err := t.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return commandTag.RowsAffected(), nil
}

func (t *txWrapper) Getx(ctx context.Context, dest interface{}, sqlizer Sqlizer) error {
	sql, args, err := sqlizer.ToSql()
	if err != nil {
		return err
	}
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	if err = pgxscan.ScanOne(dest, rows); err != nil {
		return err
	}
	return rows.Err()
}

func (t *txWrapper) Selectx(ctx context.Context, dest interface{}, sqlizer Sqlizer) error {
	sql, args, err := sqlizer.ToSql()
	if err != nil {
		return err
	}
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	if err = pgxscan.ScanAll(dest, rows); err != nil {
		return err
	}

	return rows.Err()
}

var _ ExecutorRunner = &txWrapper{}
