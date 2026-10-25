package types

import (
	"fmt"
	"reflect"
)

// AssertNoFloatMoney walks v and returns an error if any exported float64 field
// looks like a money field (Amount, Balance, …). Prefer testkit.BanFloat64Money
// in tests; this helper is for app startup checks.
func AssertNoFloatMoney(v any) error {
	return walkBanFloat(reflect.ValueOf(v), typeName(v))
}

func typeName(v any) string {
	t := reflect.TypeOf(v)
	if t == nil {
		return "?"
	}
	return t.String()
}

func walkBanFloat(v reflect.Value, path string) error {
	if !v.IsValid() {
		return nil
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return walkBanFloat(v.Elem(), path)
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			fp := path + "." + f.Name
			if f.Type.Kind() == reflect.Float64 && looksLikeMoney(f.Name) {
				return fmt.Errorf("types: float64 money field %s forbidden (use types.Money)", fp)
			}
			if err := walkBanFloat(v.Field(i), fp); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if err := walkBanFloat(v.Index(i), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func looksLikeMoney(name string) bool {
	switch name {
	case "Amount", "Balance", "Money", "Credit", "Debit", "Price", "Fee", "Total":
		return true
	default:
		return false
	}
}
