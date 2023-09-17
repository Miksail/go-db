package dbscan

// APIOption is a function type that changes API configuration.
type APIOption func(api *API)

// WithScannableTypes specifies a list of interfaces that underlying database library can scan into.
// In case the destination type passed to dbscan implements one of those interfaces,
// dbscan will handle it as primitive type case i.e. simply pass the destination to the database library.
// Instead of attempting to map database columns to destination struct fields or map keys.
// In order for reflection to capture the interface type, you must pass it by pointer.
//
// For example your database library defines a scanner interface like this:
//
//	type Scanner interface {
//	    Scan(...) error
//	}
//
// You can pass it to dbscan this way:
// dbscan.WithScannableTypes((*Scanner)(nil)).
func WithScannableTypes(scannableTypes ...interface{}) APIOption {
	return func(api *API) {
		api.scannableTypesOption = scannableTypes
	}
}
