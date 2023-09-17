package balancer

import (
	"github.com/Masterminds/squirrel"
)

// PgQb sets placeholder format
func PgQb() squirrel.StatementBuilderType {
	return squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
}
