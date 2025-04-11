// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package sql implements utils for sql database.
package sql

import (
	"errors"
	"regexp"
)

var (
	errValidation    = errors.New("entity name must contains only letters, numbers and underscore symbol")
	entityNameRegexp = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)
)

// ValidateEntityName can be used to validate database/table/column name.
func ValidateEntityName(s string) error {
	if !entityNameRegexp.MatchString(s) {
		return errValidation
	}

	return nil
}

// QuotedDatabaseEntity ...
// Arguments must be sanitized.
func QuotedDatabaseEntity(database, entity string) string {
	return QuotedEntity(database) + "." + QuotedEntity(entity)
}

// QuotedEntity ...
// Arguments must be sanitized.
func QuotedEntity(entity string) string {
	return `"` + entity + `"`
}
