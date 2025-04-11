// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package database

//go:generate go run github.com/alvaroloes/enumer -type=QueryErrorType -trimprefix=Err -output=error_enum.go

// QueryErrorType ...
type QueryErrorType uint8

const (
	// ErrUnknownTable means that the table has not been created.
	// Equivalent to ClickHouse server error code 60.
	ErrUnknownTable QueryErrorType = iota + 1
	// ErrTableAlreadyExists means that the table already exists.
	// Equivalent to ClickHouse server error code 57.
	ErrTableAlreadyExists
)

// QueryError is needed because different drivers
// return different types of errors.
// This type allows the 'rollup' package to understand errors from the driver.
// You must be sure to wrap the driver error in the Shard,
// Row, Rows methods into QueryError.
// Check example for 'clickhouse-go' driver at the [github.com/ozontech/ch-rollup/pkg/database/error_wrappers/clickhouse_go.Wrap]
type QueryError struct {
	Type  QueryErrorType
	Inner error
}

func (e QueryError) Error() string {
	if e.Type.IsAQueryErrorType() {
		return e.Type.String()
	}

	return "Unknown"
}

func (e QueryError) Unwrap() error {
	return e.Inner
}
