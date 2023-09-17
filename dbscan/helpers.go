package dbscan

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// NameMapperFunc is a function type that maps a struct field name to the database column name.
type NameMapperFunc func(string) string

var (
	matchFirstCapRe = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCapRe   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

// SnakeCaseMapper is a NameMapperFunc that maps struct field to snake case.
func SnakeCaseMapper(str string) string {
	snake := matchFirstCapRe.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCapRe.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// NotFound returns true if err is a not found error.
// This error is returned by ScanOne if there were no rows.
func NotFound(err error) bool {
	return errors.Is(err, errNotFound)
}

var errNotFound = errors.New("scany: no row was found")

type sliceDestinationMeta struct {
	val             reflect.Value
	elementBaseType reflect.Type
	elementByPtr    bool
}

func scanSliceElement(rs *RowScanner, sliceMeta *sliceDestinationMeta) error {
	dstValPtr := reflect.New(sliceMeta.elementBaseType)
	if err := rs.Scan(dstValPtr.Interface()); err != nil {
		return err
	}
	var elemVal reflect.Value
	if sliceMeta.elementByPtr {
		elemVal = dstValPtr
	} else {
		elemVal = dstValPtr.Elem()
	}

	sliceMeta.val.Set(reflect.Append(sliceMeta.val, elemVal))
	return nil
}

func (api *API) isScannableType(dstType reflect.Type) bool {
	dstRefType := reflect.PtrTo(dstType)
	for _, st := range api.scannableTypesReflect {
		if dstRefType.Implements(st) || dstType.Implements(st) {
			return true
		}
	}
	return false
}

func parseDestination(dst interface{}) (reflect.Value, error) {
	dstVal := reflect.ValueOf(dst)

	if !dstVal.IsValid() || (dstVal.Kind() == reflect.Ptr && dstVal.IsNil()) {
		return reflect.Value{}, errors.Errorf("scany: destination must be a non nil pointer")
	}
	if dstVal.Kind() != reflect.Ptr {
		return reflect.Value{}, errors.Errorf("scany: destination must be a pointer, got: %v", dstVal.Type())
	}

	dstVal = dstVal.Elem()
	return dstVal, nil
}
