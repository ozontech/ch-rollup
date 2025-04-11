// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package rollup

import (
	"fmt"
	"time"

	"github.com/huandu/go-sqlbuilder"

	sliceUtils "github.com/ozontech/ch-rollup/internal/utils/slice"
	sqlUtils "github.com/ozontech/ch-rollup/internal/utils/sql"
	timeUtils "github.com/ozontech/ch-rollup/internal/utils/time"
	"github.com/ozontech/ch-rollup/pkg/types"
)

// generateRollUpStatementOptions. All options must be sanitized to prevent sql-injection.
type generateRollUpStatementOptions struct {
	Database  string
	FromTable string
	ToTable   string
	Interval  time.Duration
	Columns   []types.ColumnSetting
}

func generateRollUpStatement(opts generateRollUpStatementOptions) string {
	timeColumnName := getTimeColumnName(opts.Columns)

	ib := sqlbuilder.NewInsertBuilder().InsertInto(sqlUtils.QuotedDatabaseEntity(opts.Database, opts.ToTable))
	ib.Cols(generateRollupInsertColumnsStatement(opts.Columns)...)

	sb := ib.Select(
		generateRollupSelectStatement(
			generateIntervalStatement(timeColumnName, opts.Interval),
			opts.Columns,
		)...,
	)

	sb.From(sqlUtils.QuotedDatabaseEntity(opts.Database, opts.FromTable))

	// We will fill placeholders with time at Exec()
	sb.Where(
		sb.GreaterEqualThan(sqlUtils.QuotedDatabaseEntity(opts.FromTable, timeColumnName), ""),
		sb.LessThan(sqlUtils.QuotedDatabaseEntity(opts.FromTable, timeColumnName), ""),
	)

	sb.GroupBy(generateGroupByStatement(opts.Columns)...)

	sql, _ := ib.BuildWithFlavor(sqlbuilder.ClickHouse)
	return sql
}

func generateRollupInsertColumnsStatement(columns []types.ColumnSetting) []string {
	return sliceUtils.ConvertFuncWithSkip(
		columns,
		func(elem types.ColumnSetting) (string, bool) {
			return sqlUtils.QuotedEntity(elem.Name), false
		},
	)
}

func generateRollupSelectStatement(intervalStatement string, columns []types.ColumnSetting) []string {
	return sliceUtils.ConvertFuncWithSkip(
		columns,
		func(elem types.ColumnSetting) (string, bool) {
			if elem.IsRollUpTime {
				return intervalStatement, false
			}

			if elem.Expression == "" {
				return sqlUtils.QuotedEntity(elem.Name), false
			}

			return elem.Expression, false
		},
	)
}

func generateGroupByStatement(columns []types.ColumnSetting) []string {
	return sliceUtils.ConvertFuncWithSkip(
		columns,
		func(elem types.ColumnSetting) (string, bool) {
			return sqlUtils.QuotedEntity(elem.Name), elem.Expression != ""
		},
	)
}

func generateIntervalStatement(timeColumn string, interval time.Duration) string {
	return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d SECOND) as %s", sqlUtils.QuotedEntity(timeColumn), timeUtils.SecondsFromDuration(interval), sqlUtils.QuotedEntity(timeColumn))
}

func getTimeColumnName(columns []types.ColumnSetting) string {
	for _, col := range columns {
		if col.IsRollUpTime {
			return col.Name
		}
	}

	return ""
}
