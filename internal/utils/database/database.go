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
func CreateTableAs(ctx context.Context, shard database.Shard, database, srcTable, dstTable string) error {
	if sqlUtils.ValidateEntityName(database) != nil || sqlUtils.ValidateEntityName(srcTable) != nil || sqlUtils.ValidateEntityName(dstTable) != nil {
		return errInvalidArguments
	}

	if err := shard.Exec(ctx, fmt.Sprintf("CREATE TABLE %s AS %s", sqlUtils.QuotedDatabaseEntity(database, dstTable), sqlUtils.QuotedDatabaseEntity(database, srcTable))); err != nil {
		return fmt.Errorf("failed to create table %s as %s in %s: %w", dstTable, srcTable, database, err)
	}

	return nil
}

// DropTable ...
func DropTable(ctx context.Context, shard database.Shard, database, table string) error {
	if sqlUtils.ValidateEntityName(database) != nil || sqlUtils.ValidateEntityName(table) != nil {
		return errInvalidArguments
	}

	if err := shard.Exec(ctx, "DROP TABLE "+sqlUtils.QuotedDatabaseEntity(database, table)); err != nil {
		return fmt.Errorf("failed to drop table %s in %s: %w", table, database, err)
	}

	return nil
}
