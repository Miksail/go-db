package dbscan

import (
	"reflect"

	"github.com/pkg/errors"
)

// Rows is an abstract database rows that dbscan can iterate over and get the data from.
// This interface is used to decouple from any particular database library.
type Rows interface {
	Close() error
	Err() error
	Next() bool
	Columns() ([]string, error)
	Scan(dest ...interface{}) error
}

// API is the core type in dbscan. It implements all the logic and exposes functionality available in the package.
// With API type users can create a custom API instance and override default settings hence configure dbscan.
type API struct {
	structTagKey          string
	columnSeparator       string
	fieldMapperFn         NameMapperFunc
	scannableTypesOption  []interface{}
	scannableTypesReflect []reflect.Type
}

func (api *API) ScanAll(dst interface{}, rows Rows) error {
	return api.processRows(dst, rows, true)
}

func (api *API) ScanOne(dst interface{}, rows Rows) error {
	return api.processRows(dst, rows, false)
}

// NewAPI creates a new API object with provided list of options.
func NewAPI(opts ...APIOption) (*API, error) {
	api := &API{
		structTagKey:    "db",
		columnSeparator: ".",
		fieldMapperFn:   SnakeCaseMapper,
	}
	for _, o := range opts {
		o(api)
	}
	for _, stOpt := range api.scannableTypesOption {
		st := reflect.TypeOf(stOpt)
		if st == nil {
			return nil, errors.Errorf("scany: scannable type must be a pointer, got %T", st)
		}
		if st.Kind() != reflect.Ptr {
			return nil, errors.Errorf("scany: scannable type must be a pointer, got %s: %s",
				st.Kind(), st.String())
		}
		st = st.Elem()
		if st.Kind() != reflect.Interface {
			return nil, errors.Errorf("scany: scannable type must be a pointer to an interface, got %s: %s",
				st.Kind(), st.String())
		}
		api.scannableTypesReflect = append(api.scannableTypesReflect, st)
	}
	return api, nil
}

func (api *API) processRows(dst interface{}, rows Rows, multipleRows bool) error {
	defer rows.Close() // nolint: errcheck
	var sliceMeta *sliceDestinationMeta
	if multipleRows {
		var err error
		sliceMeta, err = api.parseSliceDestination(dst)
		if err != nil {
			return err
		}
		// Make sure slice is empty.
		sliceMeta.val.Set(sliceMeta.val.Slice(0, 0))
	}
	rs := api.NewRowScanner(rows)
	var rowsAffected int
	for rows.Next() {
		var err error
		if multipleRows {
			err = scanSliceElement(rs, sliceMeta)
		} else {
			err = rs.Scan(dst)
		}
		if err != nil {
			return err
		}
		rowsAffected++
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "scany: rows final error")
	}

	if err := rows.Close(); err != nil {
		return errors.Wrap(err, "scany: close rows after processing")
	}

	exactlyOneRow := !multipleRows
	if exactlyOneRow {
		if rowsAffected == 0 {
			return errNotFound
		} else if rowsAffected > 1 {
			return errors.Errorf("scany: expected 1 row, got: %d", rowsAffected)
		}
	}
	return nil
}

func (api *API) parseSliceDestination(dst interface{}) (*sliceDestinationMeta, error) {
	dstValue, err := parseDestination(dst)
	if err != nil {
		return nil, err
	}

	dstType := dstValue.Type()

	if dstValue.Kind() != reflect.Slice {
		return nil, errors.Errorf(
			"scany: destination must be a slice, got: %v", dstType,
		)
	}

	elementBaseType := dstType.Elem()
	var elementByPtr bool
	// If it's a slice of pointers to structs,
	// we handle it the same way as it would be slice of struct by value
	// and dereference pointers to values,
	// because eventually we work with fields.
	// But if it's a slice of primitive type e.g. or []string or []*string,
	// we must leave and pass elements as is to Rows.Scan().
	if elementBaseType.Kind() == reflect.Ptr {
		elementBaseTypeElem := elementBaseType.Elem()
		if elementBaseTypeElem.Kind() == reflect.Struct && !api.isScannableType(elementBaseType) {
			elementBaseType = elementBaseTypeElem
			elementByPtr = true
		}
	}

	meta := &sliceDestinationMeta{
		val:             dstValue,
		elementBaseType: elementBaseType,
		elementByPtr:    elementByPtr,
	}
	return meta, nil
}
