// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package rollup

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/ozontech/ch-rollup/pkg/database"
	"github.com/ozontech/ch-rollup/pkg/database/mock"
	"github.com/ozontech/ch-rollup/pkg/types"
)

func TestNew(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	clusterMock := mock.NewMockCluster(ctrl)

	assert.Equal(
		t,
		&RollUp{
			cluster: clusterMock,
		},
		New(clusterMock),
	)
}

func TestRunOptions_Validate(t *testing.T) {
	t.Parallel()

	const (
		testDatabase     = "test_database"
		testTable        = "test_table"
		testTempTable    = "test_temp_table"
		testPartitionKey = time.Hour
		testInterval     = time.Hour * 2
		testAfter        = time.Hour * 4
		testCopyInterval = time.Hour
	)

	var (
		testColumns = []types.ColumnSetting{
			{
				Name: "test",
			},
			{
				Name:       "test_with_expression",
				Expression: "countMergeState(test_with_expression)",
			},
			{
				Name:         "test_time",
				IsRollUpTime: true,
			},
		}
	)

	type fields struct {
		Database     string
		Table        string
		TempTable    string
		PartitionKey time.Duration
		Columns      []types.ColumnSetting
		Interval     time.Duration
		After        time.Duration
		CopyInterval time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Ok",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
		},
		{
			name: "Bad database",
			fields: fields{
				Database:     "$bad_database",
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Bad table",
			fields: fields{
				Database:     testDatabase,
				Table:        "bad-table",
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Bad temp table",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    "bad_temp_table'",
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Bad partition key",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: -time.Hour,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Bad interval",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     0,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Bad after",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        -time.Second,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Bad copy interval",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: 0,
			},
			wantErr: true,
		},
		{
			name: "Bad column",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns: []types.ColumnSetting{
					{
						Name: "",
					},
				},
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Time column not exists",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns: []types.ColumnSetting{
					{
						Name: "test",
					},
				},
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := RunOptions{
				Database:     tt.fields.Database,
				Table:        tt.fields.Table,
				TempTable:    tt.fields.TempTable,
				PartitionKey: tt.fields.PartitionKey,
				Columns:      tt.fields.Columns,
				Interval:     tt.fields.Interval,
				After:        tt.fields.After,
				CopyInterval: tt.fields.CopyInterval,
			}

			assert.Equal(t, tt.wantErr, opts.validate() != nil)
		})
	}
}

//nolint:paralleltest
func TestRollUp_Run(t *testing.T) {
	defer func() {
		timeNow = time.Now
	}()

	const (
		testDatabase     = "test_database"
		testTable        = "test_table"
		testTempTable    = "test_temp_table"
		testPartitionKey = time.Hour * 24
		testInterval     = time.Hour
		testAfter        = time.Hour * 24
		testCopyInterval = time.Hour

		testShardName = "test-shard"
		testPartition = "test-partition"
	)

	var (
		testColumns = []types.ColumnSetting{
			{
				Name: "test",
			},
			{
				Name:       "test_with_expression",
				Expression: "countMergeState(test_with_expression)",
			},
			{
				Name:         "test_time",
				IsRollUpTime: true,
			},
		}

		testPreviousRollup = time.Date(2024, time.June, 23, 0, 0, 0, 0, time.UTC)
		testRollupTo       = time.Date(2024, time.June, 24, 0, 0, 0, 0, time.UTC)
		testCurrentTime    = time.Date(2024, time.June, 25, 10, 0, 0, 0, time.UTC)
	)

	tests := []struct {
		name        string
		prepareMock func(ctrl *gomock.Controller) database.Cluster
		opts        RunOptions
		wantErr     bool
	}{
		{
			name: "Ok",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPreviousRollup)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`INSERT INTO "test_database"."test_temp_table" ("test", "test_with_expression", "test_time") SELECT "test", countMergeState(test_with_expression), toStartOfInterval("test_time", INTERVAL 3600 SECOND) as "test_time" FROM "test_database"."test_table" WHERE "test_table"."test_time" >= ? AND "test_table"."test_time" < ? GROUP BY "test", "test_time"`,
					gomock.Any(),
					gomock.Any(),
				).Times(24)

				rowsMock := mock.NewMockRows(ctrl)

				rowsMock.EXPECT().Next().Return(true)
				rowsMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPartition)
				rowsMock.EXPECT().Next()
				rowsMock.EXPECT().Err()
				rowsMock.EXPECT().Close()

				shardMock.EXPECT().Query(
					gomock.Any(),
					"SELECT partition FROM system.parts WHERE database = ? AND table = ? AND active = ? GROUP BY partition",
					testDatabase,
					testTempTable,
					1,
				).Return(rowsMock, nil)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`ALTER TABLE "test_database"."test_table" REPLACE PARTITION ? FROM "test_database"."test_temp_table"`,
					"test-partition",
				)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					"INSERT INTO rollup_meta_info (database, table, after_sec, interval_sec, roll_ups_at) VALUES (?, ?, ?, ?, ?)",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
					testRollupTo,
				)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`DROP TABLE "test_database"."test_temp_table"`,
				)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
		},
		{
			name: "No need to rollup",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testCurrentTime)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
		},
		{
			name: "Not initialized",
			prepareMock: func(_ *gomock.Controller) database.Cluster {
				return nil
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Validation failed",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				return mock.NewMockCluster(ctrl)
			},
			opts: RunOptions{
				Database:     "bad%database",
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Failed to get shards",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				clusterMock := mock.NewMockCluster(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return(nil, errors.New("test-error"))

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Error on getLatestRollUpByKeyOnShard",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).Return(errors.New("test-error"))
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Name().Return(testShardName)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "First rollup (meta info not exists)",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).Return(sql.ErrNoRows)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					"INSERT INTO rollup_meta_info (database, table, after_sec, interval_sec, roll_ups_at) VALUES (?, ?, ?, ?, ?)",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
					testCurrentTime.Truncate(testPartitionKey),
				)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
		},
		{
			name: "Meta info table not exist with ok create",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).
					Return(
						database.QueryError{
							Type: database.ErrUnknownTable,
						},
					)

				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					rollUpMetaInfoTableDefinition,
				)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					"INSERT INTO rollup_meta_info (database, table, after_sec, interval_sec, roll_ups_at) VALUES (?, ?, ?, ?, ?)",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
					testCurrentTime.Truncate(testPartitionKey),
				)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
		},
		{
			name: "Meta info table not exist with failed to create",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).
					Return(
						database.QueryError{
							Type: database.ErrUnknownTable,
						},
					)

				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					rollUpMetaInfoTableDefinition,
				).Return(errors.New("test-error"))

				shardMock.EXPECT().Name().Return(testShardName)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Temp table already exists with ok recreation",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPreviousRollup)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`).Return(database.QueryError{
					Type: database.ErrTableAlreadyExists,
				})
				shardMock.EXPECT().Exec(gomock.Any(), `DROP TABLE "test_database"."test_temp_table"`)
				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`INSERT INTO "test_database"."test_temp_table" ("test", "test_with_expression", "test_time") SELECT "test", countMergeState(test_with_expression), toStartOfInterval("test_time", INTERVAL 3600 SECOND) as "test_time" FROM "test_database"."test_table" WHERE "test_table"."test_time" >= ? AND "test_table"."test_time" < ? GROUP BY "test", "test_time"`,
					gomock.Any(),
					gomock.Any(),
				).Times(24)

				rowsMock := mock.NewMockRows(ctrl)

				rowsMock.EXPECT().Next().Return(true)
				rowsMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPartition)
				rowsMock.EXPECT().Next()
				rowsMock.EXPECT().Err()
				rowsMock.EXPECT().Close()

				shardMock.EXPECT().Query(
					gomock.Any(),
					"SELECT partition FROM system.parts WHERE database = ? AND table = ? AND active = ? GROUP BY partition",
					testDatabase,
					testTempTable,
					1,
				).Return(rowsMock, nil)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`ALTER TABLE "test_database"."test_table" REPLACE PARTITION ? FROM "test_database"."test_temp_table"`,
					"test-partition",
				)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					"INSERT INTO rollup_meta_info (database, table, after_sec, interval_sec, roll_ups_at) VALUES (?, ?, ?, ?, ?)",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
					testRollupTo,
				)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`DROP TABLE "test_database"."test_temp_table"`,
				)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
		},
		{
			name: "Failed to create temp table with unknown error",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPreviousRollup)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`).Return(errors.New("unknown-error"))
				shardMock.EXPECT().Name().Return(testShardName)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Temp table already exists, but recreation was failed on drop",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPreviousRollup)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`).Return(database.QueryError{
					Type: database.ErrTableAlreadyExists,
				})

				shardMock.EXPECT().Exec(gomock.Any(), `DROP TABLE "test_database"."test_temp_table"`).Return(errors.New("test-error"))

				shardMock.EXPECT().Name().Return(testShardName)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Temp table already exists, but recreation was failed on create",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPreviousRollup)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`).Return(database.QueryError{
					Type: database.ErrTableAlreadyExists,
				})
				shardMock.EXPECT().Exec(gomock.Any(), `DROP TABLE "test_database"."test_temp_table"`)
				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`).Return(errors.New("test-error"))

				shardMock.EXPECT().Name().Return(testShardName)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Failed to insert data",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPreviousRollup)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`INSERT INTO "test_database"."test_temp_table" ("test", "test_with_expression", "test_time") SELECT "test", countMergeState(test_with_expression), toStartOfInterval("test_time", INTERVAL 3600 SECOND) as "test_time" FROM "test_database"."test_table" WHERE "test_table"."test_time" >= ? AND "test_table"."test_time" < ? GROUP BY "test", "test_time"`,
					gomock.Any(),
					gomock.Any(),
				).Return(errors.New("test-error"))

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`DROP TABLE "test_database"."test_temp_table"`,
				)

				shardMock.EXPECT().Name().Return(testShardName)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Failed to get partitions",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPreviousRollup)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`INSERT INTO "test_database"."test_temp_table" ("test", "test_with_expression", "test_time") SELECT "test", countMergeState(test_with_expression), toStartOfInterval("test_time", INTERVAL 3600 SECOND) as "test_time" FROM "test_database"."test_table" WHERE "test_table"."test_time" >= ? AND "test_table"."test_time" < ? GROUP BY "test", "test_time"`,
					gomock.Any(),
					gomock.Any(),
				).Times(24)

				rowsMock := mock.NewMockRows(ctrl)

				rowsMock.EXPECT().Next().Return(true)
				rowsMock.EXPECT().Scan(gomock.Any()).Return(errors.New("test-error"))
				rowsMock.EXPECT().Close()

				shardMock.EXPECT().Query(
					gomock.Any(),
					"SELECT partition FROM system.parts WHERE database = ? AND table = ? AND active = ? GROUP BY partition",
					testDatabase,
					testTempTable,
					1,
				).Return(rowsMock, nil)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`DROP TABLE "test_database"."test_temp_table"`,
				)

				shardMock.EXPECT().Name().Return(testShardName)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
		{
			name: "Failed to replace partitions",
			prepareMock: func(ctrl *gomock.Controller) database.Cluster {
				timeNow = func() time.Time {
					return testCurrentTime
				}

				clusterMock := mock.NewMockCluster(ctrl)
				shardMock := mock.NewMockShard(ctrl)

				clusterMock.EXPECT().Shards(gomock.Any()).Return([]database.Shard{shardMock}, nil)

				rowMock := mock.NewMockRow(ctrl)
				rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPreviousRollup)
				shardMock.EXPECT().QueryRow(
					gomock.Any(),
					"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
					testDatabase,
					testTable,
					int(testAfter.Seconds()),
					int(testInterval.Seconds()),
				).Return(rowMock)

				shardMock.EXPECT().Exec(gomock.Any(), `CREATE TABLE "test_database"."test_temp_table" AS "test_database"."test_table"`)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`INSERT INTO "test_database"."test_temp_table" ("test", "test_with_expression", "test_time") SELECT "test", countMergeState(test_with_expression), toStartOfInterval("test_time", INTERVAL 3600 SECOND) as "test_time" FROM "test_database"."test_table" WHERE "test_table"."test_time" >= ? AND "test_table"."test_time" < ? GROUP BY "test", "test_time"`,
					gomock.Any(),
					gomock.Any(),
				).Times(24)

				rowsMock := mock.NewMockRows(ctrl)

				rowsMock.EXPECT().Next().Return(true)
				rowsMock.EXPECT().Scan(gomock.Any()).SetArg(0, testPartition)
				rowsMock.EXPECT().Next()
				rowsMock.EXPECT().Err()
				rowsMock.EXPECT().Close()

				shardMock.EXPECT().Query(
					gomock.Any(),
					"SELECT partition FROM system.parts WHERE database = ? AND table = ? AND active = ? GROUP BY partition",
					testDatabase,
					testTempTable,
					1,
				).Return(rowsMock, nil)

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`ALTER TABLE "test_database"."test_table" REPLACE PARTITION ? FROM "test_database"."test_temp_table"`,
					"test-partition",
				).Return(errors.New("test-error"))

				shardMock.EXPECT().Exec(
					gomock.Any(),
					`DROP TABLE "test_database"."test_temp_table"`,
				)

				shardMock.EXPECT().Name().Return(testShardName)

				return clusterMock
			},
			opts: RunOptions{
				Database:     testDatabase,
				Table:        testTable,
				TempTable:    testTempTable,
				PartitionKey: testPartitionKey,
				Columns:      testColumns,
				Interval:     testInterval,
				After:        testAfter,
				CopyInterval: testCopyInterval,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		timeNow = time.Now

		t.Run(tt.name, func(t *testing.T) {
			defer goleak.VerifyNone(t)

			ctrl := gomock.NewController(t)

			s := &RollUp{
				cluster: tt.prepareMock(ctrl),
			}

			assert.Equal(t, tt.wantErr, s.Run(context.Background(), tt.opts) != nil)
		})
	}
}
