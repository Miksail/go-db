package balancer

import (
	"context"
	"fmt"
)

func ExecInTx(ctx context.Context, balancer TransactionalBalancer, handler func(context.Context) error) error {
	ctx, err := balancer.BeginTransaction(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		err = balancer.RollbackTransaction(ctx)
		if err != nil {
			// todo log
		}
	}()

	if err = handler(ctx); err != nil {
		return err
	}

	if err = balancer.CommitTransaction(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func GetInTx[T any](ctx context.Context, balancer TransactionalBalancer, handler func(context.Context) (T, error)) (T, error) {
	var res T
	ctx, err := balancer.BeginTransaction(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		err = balancer.RollbackTransaction(ctx)
		if err != nil {
			// todo log
		}
	}()

	if res, err = handler(ctx); err != nil {
		return res, err
	}

	if err = balancer.CommitTransaction(ctx); err != nil {
		return res, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return res, nil
}
