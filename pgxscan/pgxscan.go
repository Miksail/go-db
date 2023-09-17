package pgxscan

import (
	"database/sql"

	"github.com/Miksail/go-db/dbscan"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// DefaultAPI is the default instance of API with all configuration settings set to default.
var DefaultAPI = mustNewAPI()

// ScanAll is a package-level helper function that uses the DefaultAPI object.
// See API.ScanAll for details.
func ScanAll(dst interface{}, rows pgx.Rows) error {
	return DefaultAPI.ScanAll(dst, NewRowsAdapter(rows))
}

// ScanOne is a package-level helper function that uses the DefaultAPI object.
// See API.ScanOne for details.
func ScanOne(dst interface{}, rows pgx.Rows) error {
	err := DefaultAPI.ScanOne(dst, NewRowsAdapter(rows))
	if dbscan.NotFound(err) {
		return pgx.ErrNoRows
	}
	return err
}

func mustNewAPI(opts ...dbscan.APIOption) *dbscan.API {
	defaultOpts := []dbscan.APIOption{
		dbscan.WithScannableTypes(
			(*sql.Scanner)(nil),
			(*pgtype.BinaryDecoder)(nil),
			(*pgtype.TextDecoder)(nil),
		),
	}
	opts = append(defaultOpts, opts...)
	api, err := dbscan.NewAPI(opts...)
	if err != nil {
		panic(err)
	}
	return api
}
