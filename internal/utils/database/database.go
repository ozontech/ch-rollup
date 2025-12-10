// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package database implements utils for database.
package database

import (
	"context"
	"errors"
	"fmt"

	sqlUtils "github.com/ozontech/ch-rollup/internal/utils/sql"
	"github.com/ozontech/ch-rollup/pkg/database"
)

var errInvalidArguments = errors.New("invalid arguments")

// CreateTableAs ...
func CreateTableAs(ctx context.Context, shard database.Shard, databaseName, srcTableName, dstTableName string) error {
	if sqlUtils.ValidateEntityName(databaseName) != nil || sqlUtils.ValidateEntityName(srcTableName) != nil || sqlUtils.ValidateEntityName(dstTableName) != nil {
		return errInvalidArguments
	}

	if err := shard.Exec(ctx, fmt.Sprintf("CREATE TABLE %s AS %s", sqlUtils.QuotedDatabaseEntity(databaseName, dstTableName), sqlUtils.QuotedDatabaseEntity(databaseName, srcTableName))); err != nil {
		return fmt.Errorf("failed to create table %s as %s in %s: %w", dstTableName, srcTableName, databaseName, err)
	}

	return nil
}

// DropTable ...
func DropTable(ctx context.Context, shard database.Shard, databaseName, tableName string) error {
	if sqlUtils.ValidateEntityName(databaseName) != nil || sqlUtils.ValidateEntityName(tableName) != nil {
		return errInvalidArguments
	}

	if err := shard.Exec(ctx, "DROP TABLE "+sqlUtils.QuotedDatabaseEntity(databaseName, tableName)); err != nil {
		return fmt.Errorf("failed to drop table %s in %s: %w", tableName, databaseName, err)
	}

	return nil
}
