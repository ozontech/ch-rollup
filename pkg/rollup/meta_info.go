// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package rollup

import (
	"context"
	"time"

	"github.com/huandu/go-sqlbuilder"

	timeUtils "github.com/ozontech/ch-rollup/internal/utils/time"
	"github.com/ozontech/ch-rollup/pkg/database"
)

type metaInfo struct {
	Database  string
	Table     string
	After     time.Duration
	Interval  time.Duration
	RollUpsAt time.Time
}

type metaInfoKey struct {
	Database string
	Table    string
	After    time.Duration
	Interval time.Duration
}

func getLatestRollUpByKeyOnShard(ctx context.Context, shard database.Shard, key metaInfoKey) (time.Time, error) {
	var rollUpsAt time.Time

	sb := sqlbuilder.NewSelectBuilder().From("rollup_meta_info")
	sb.Select("max(roll_ups_at)")
	sb.Where(
		sb.Equal("database", key.Database),
		sb.Equal("table", key.Table),
		sb.Equal("after_sec", timeUtils.SecondsFromDuration(key.After)),
		sb.Equal("interval_sec", timeUtils.SecondsFromDuration(key.Interval)),
	)
	sb.GroupBy("database", "table", "after_sec", "interval_sec")

	sql, args := sb.BuildWithFlavor(sqlbuilder.ClickHouse)

	err := shard.QueryRow(ctx, sql, args...).Scan(&rollUpsAt)
	if err != nil {
		return time.Time{}, err
	}

	return rollUpsAt, nil
}

func addMetaInfoOnShard(ctx context.Context, shard database.Shard, metaInfo metaInfo) error {
	ib := sqlbuilder.NewInsertBuilder().InsertInto("rollup_meta_info")
	ib.Cols("database", "table", "after_sec", "interval_sec", "roll_ups_at")
	ib.Values(
		metaInfo.Database,
		metaInfo.Table,
		timeUtils.SecondsFromDuration(metaInfo.After),
		timeUtils.SecondsFromDuration(metaInfo.Interval),
		metaInfo.RollUpsAt,
	)

	sql, args := ib.BuildWithFlavor(sqlbuilder.ClickHouse)

	return shard.Exec(ctx, sql, args...)
}
