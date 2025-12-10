// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package rollup implements rollup logic.
package rollup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	databaseUtils "github.com/ozontech/ch-rollup/internal/utils/database"
	sqlUtils "github.com/ozontech/ch-rollup/internal/utils/sql"
	timeUtils "github.com/ozontech/ch-rollup/internal/utils/time"
	"github.com/ozontech/ch-rollup/pkg/database"
	"github.com/ozontech/ch-rollup/pkg/types"
)

const (
	rollUpMetaInfoTableDefinition = `
			CREATE TABLE IF NOT EXISTS rollup_meta_info(
				database String,
				table String,
				after_sec UInt64,
				interval_sec UInt64,
				roll_ups_at DateTime
			) ENGINE = MergeTree() ORDER BY (database, table, after_sec, interval_sec, roll_ups_at);
	`
)

var (
	// timeNow used for testing reasons.
	timeNow = time.Now
)

// RollUp ...
type RollUp struct {
	cluster database.Cluster
}

// New returns new RollUp.
func New(cluster database.Cluster) *RollUp {
	return &RollUp{
		cluster: cluster,
	}
}

// RunOptions ...
type RunOptions struct {
	Database     string
	Table        string
	TempTable    string
	PartitionKey time.Duration
	Columns      []types.ColumnSetting
	Interval     time.Duration
	After        time.Duration
	CopyInterval time.Duration
}

const (
	defaultCopyInterval = time.Hour
)

func (opts *RunOptions) setDefaults() {
	if opts.CopyInterval <= 0 {
		opts.CopyInterval = defaultCopyInterval
	}
}

var (
	errBadPartitionKey    = errors.New("partitionKey must be greater then 0")
	errBadInterval        = errors.New("interval must be greater then 0")
	errBadAfter           = errors.New("after must be greater then 0")
	errBadCopyInterval    = errors.New("copyInterval must be greater then 0")
	errTimeColumnNotFound = errors.New("you must specify column with isRollUpTime option")
)

func (opts *RunOptions) validate() error {
	if err := sqlUtils.ValidateEntityName(opts.Database); err != nil {
		return fmt.Errorf("failed to validate database: %w", err)
	}

	if err := sqlUtils.ValidateEntityName(opts.Table); err != nil {
		return fmt.Errorf("failed to validate table name: %w", err)
	}

	if err := sqlUtils.ValidateEntityName(opts.TempTable); err != nil {
		return fmt.Errorf("failed to validate tempTable name: %w", err)
	}

	if opts.PartitionKey <= 0 {
		return errBadPartitionKey
	}

	if opts.Interval <= 0 {
		return errBadInterval
	}

	if opts.After <= 0 {
		return errBadAfter
	}

	if opts.CopyInterval <= 0 {
		return errBadCopyInterval
	}

	for index, column := range opts.Columns {
		if err := column.Validate(); err != nil {
			return fmt.Errorf("failed to validate column with index %d: %w", index, err)
		}
	}

	if timeColumnName := getTimeColumnName(opts.Columns); timeColumnName == "" {
		return errTimeColumnNotFound
	}

	return nil
}

var (
	errNotInitialized = errors.New("not initialed")
)

// Run roll up on current database.Cluster with RunOptions.
func (s *RollUp) Run(ctx context.Context, opts RunOptions) error {
	if s == nil || s.cluster == nil {
		return errNotInitialized
	}

	opts.setDefaults()

	if err := opts.validate(); err != nil {
		return fmt.Errorf("failed to validate options: %w", err)
	}

	shards, err := s.cluster.Shards(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shards: %w", err)
	}

	g, eCtx := errgroup.WithContext(ctx)
	for _, shard := range shards {
		g.Go(func() error {
			err := s.runOnShard(eCtx, shard, opts)
			if err != nil {
				return fmt.Errorf("failed to run roll up on %s: %w", shard.Name(), err)
			}

			return nil
		})
	}

	return g.Wait()
}

func (s *RollUp) runOnShard(ctx context.Context, shard database.Shard, opts RunOptions) error {
	latestRollUp, err := getLatestRollUpByKeyOnShard(
		ctx,
		shard,
		metaInfoKey{
			Database: opts.Database,
			Table:    opts.Table,
			After:    opts.After,
			Interval: opts.Interval,
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return createMetaInfo(ctx, shard, timeNow().Truncate(opts.PartitionKey), opts)
		}

		var queryError database.QueryError
		if errors.As(err, &queryError) && queryError.Type == database.ErrUnknownTable {
			err = shard.Exec(ctx, rollUpMetaInfoTableDefinition)
			if err != nil {
				return err
			}

			return createMetaInfo(ctx, shard, timeNow().Truncate(opts.PartitionKey), opts)
		}

		return err
	}

	rollUpTo := timeNow().Add(-opts.After).Truncate(opts.PartitionKey)
	// We don't need to roll up data if 'rollUpTo' before 'latestRollUp' or equal.
	if rollUpTo.Compare(latestRollUp) != 1 {
		return nil
	}

	err = databaseUtils.CreateTableAs(ctx, shard, opts.Database, opts.Table, opts.TempTable)
	if err != nil {
		// if temp table already exists - we drop it
		// this handles case when app got context done at
		// copying and table not been removed at defer.
		var queryError database.QueryError
		if !errors.As(err, &queryError) || queryError.Type != database.ErrTableAlreadyExists {
			return err
		}

		if err = databaseUtils.DropTable(ctx, shard, opts.Database, opts.TempTable); err != nil {
			return err
		}

		// let's try to create temp table after drop.
		if err = databaseUtils.CreateTableAs(ctx, shard, opts.Database, opts.Table, opts.TempTable); err != nil {
			return err
		}
	}

	defer func() {
		// Drop table after all manipulations.
		// We don't need to check error here because
		// if table doesn't drop - we drop it before next rollup.
		_ = databaseUtils.DropTable(ctx, shard, opts.Database, opts.TempTable)
	}()

	// need from (latestRollUp) / to (rollUpTo) / interval (opts)
	query := generateRollUpStatement(generateRollUpStatementOptions{
		Database:  opts.Database,
		FromTable: opts.Table,
		ToTable:   opts.TempTable,
		Interval:  opts.Interval,
		Columns:   opts.Columns,
	})

	copyIntervals := timeUtils.SplitTimeRangeByInterval(
		timeUtils.Range{
			From: latestRollUp,
			To:   rollUpTo,
		},
		opts.CopyInterval,
	)

	for _, interval := range copyIntervals {
		if err = shard.Exec(ctx, query, interval.From, interval.To); err != nil {
			return err
		}
	}

	partitions, err := getPartitionsOnShard(ctx, shard, opts.Database, opts.TempTable)
	if err != nil {
		return fmt.Errorf("failed to get %s.%s partitions: %w", opts.Database, opts.Table, err)
	}

	if err = replacePartitionsOnShard(ctx, shard, opts.Database, opts.TempTable, opts.Table, partitions); err != nil {
		return fmt.Errorf("failed to replace partitions from %s.%s to %s.%s: %w", opts.Database, opts.TempTable, opts.Database, opts.Table, err)
	}

	return createMetaInfo(ctx, shard, rollUpTo, opts)
}

func createMetaInfo(ctx context.Context, shard database.Shard, rollUpsAt time.Time, opts RunOptions) error {
	return addMetaInfoOnShard(ctx, shard, metaInfo{
		Database:  opts.Database,
		Table:     opts.Table,
		After:     opts.After,
		Interval:  opts.Interval,
		RollUpsAt: rollUpsAt,
	})
}
