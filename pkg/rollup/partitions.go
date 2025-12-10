// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package rollup

import (
	"context"
	"fmt"

	"github.com/huandu/go-sqlbuilder"

	sqlUtils "github.com/ozontech/ch-rollup/internal/utils/sql"
	"github.com/ozontech/ch-rollup/pkg/database"
)

func getPartitionsOnShard(ctx context.Context, shard database.Shard, databaseName, tableName string) ([]string, error) {
	sb := sqlbuilder.NewSelectBuilder().From("system.parts")

	sb.Select("partition")
	sb.Where(
		sb.Equal("database", databaseName),
		sb.Equal("table", tableName),
		sb.Equal("active", 1),
	)
	sb.GroupBy("partition")

	sql, args := sb.BuildWithFlavor(sqlbuilder.ClickHouse)

	rows, err := shard.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	var result []string

	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var partition string

		if err = rows.Scan(&partition); err != nil {
			return nil, err
		}

		result = append(result, partition)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// replacePartitionsOnShard replaces partitions on shard.
// Arguments must be sanitized.
func replacePartitionsOnShard(ctx context.Context, shard database.Shard, databaseName, from, to string, partitions []string) error {
	// TODO: generate multistatement query.
	for _, partition := range partitions {
		b := sqlbuilder.Build(
			"ALTER TABLE $? REPLACE PARTITION $? FROM $?",
			sqlbuilder.Raw(sqlUtils.QuotedDatabaseEntity(databaseName, to)),
			partition,
			sqlbuilder.Raw(sqlUtils.QuotedDatabaseEntity(databaseName, from)),
		)

		sql, args := b.BuildWithFlavor(sqlbuilder.ClickHouse)

		err := shard.Exec(ctx, sql, args...)
		if err != nil {
			return fmt.Errorf("failed to replace partition: %w", err)
		}
	}

	return nil
}
