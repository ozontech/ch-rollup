// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package clickhouse_go implements error wrapper for 'clickhouse-go'.
package clickhouse_go

import (
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/ozontech/ch-rollup/pkg/database"
	"github.com/ozontech/ch-rollup/pkg/database/error_wrappers/common"
)

// Wrap clickhouse-go errors.
func Wrap(shard database.Shard) database.Shard {
	return common.Wrap(shard, wrapException)
}

func wrapException(err error) error {
	if err == nil {
		return nil
	}

	exception := &proto.Exception{}
	if ok := errors.As(err, &exception); !ok {
		return err
	}

	queryErrorType := convertExceptionCodeToQueryErrorType(exception.Code)
	if queryErrorType == 0 {
		return err
	}

	return database.QueryError{
		Type:  queryErrorType,
		Inner: err,
	}
}

const (
	unknownTableExceptionCode = 60
	tableAlreadyExistsCode    = 57
)

func convertExceptionCodeToQueryErrorType(code int32) database.QueryErrorType {
	switch code {
	case unknownTableExceptionCode:
		return database.ErrUnknownTable
	case tableAlreadyExistsCode:
		return database.ErrTableAlreadyExists
	}

	return 0
}
