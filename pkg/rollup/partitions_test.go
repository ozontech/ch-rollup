// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package rollup

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/ozontech/ch-rollup/pkg/database/mock"
)

func Test_getPartitionsOnShard(t *testing.T) {
	t.Parallel()

	const (
		generatedQuery = "SELECT partition FROM system.parts WHERE database = ? AND table = ? AND active = ? GROUP BY partition"

		testDatabase = "test_database"
		testTable    = "test_table"

		testFirstPartition  = "test-partition-1"
		testSecondPartition = "test-partition-2"
	)

	type args struct {
		prepareMock func(ctrl *gomock.Controller, shard *mock.MockShard)
		database    string
		table       string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "Ok",
			args: args{
				prepareMock: func(ctrl *gomock.Controller, shard *mock.MockShard) {
					rowsMock := mock.NewMockRows(ctrl)

					rowsMock.EXPECT().Next().Return(true)
					rowsMock.EXPECT().Scan(gomock.Any()).SetArg(0, testFirstPartition)

					rowsMock.EXPECT().Next().Return(true)
					rowsMock.EXPECT().Scan(gomock.Any()).SetArg(0, testSecondPartition)

					rowsMock.EXPECT().Next()
					rowsMock.EXPECT().Err()
					rowsMock.EXPECT().Close()

					shard.
						EXPECT().
						Query(
							gomock.Any(),
							generatedQuery,
							testDatabase,
							testTable,
							1,
						).
						Return(rowsMock, nil)
				},
				database: testDatabase,
				table:    testTable,
			},
			want: []string{
				testFirstPartition,
				testSecondPartition,
			},
		},
		{
			name: "Error at Query()",
			args: args{
				prepareMock: func(_ *gomock.Controller, shard *mock.MockShard) {
					shard.
						EXPECT().
						Query(
							gomock.Any(),
							"SELECT partition FROM system.parts WHERE database = ? AND table = ? AND active = ? GROUP BY partition",
							"test_database",
							"test_table",
							1,
						).
						Return(nil, errors.New("test-error"))
				},
				database: "test_database",
				table:    "test_table",
			},
			wantErr: true,
		},
		{
			name: "Error at rows.Scan()",
			args: args{
				prepareMock: func(ctrl *gomock.Controller, shard *mock.MockShard) {
					rowsMock := mock.NewMockRows(ctrl)

					rowsMock.EXPECT().Next().Return(true)
					rowsMock.EXPECT().Scan(gomock.Any()).Return(errors.New("test-error"))
					rowsMock.EXPECT().Close()

					shard.
						EXPECT().
						Query(
							gomock.Any(),
							generatedQuery,
							testDatabase,
							testTable,
							1,
						).
						Return(rowsMock, nil)
				},
				database: testDatabase,
				table:    testTable,
			},
			wantErr: true,
		},
		{
			name: "Error at rows.Err()",
			args: args{
				prepareMock: func(ctrl *gomock.Controller, shard *mock.MockShard) {
					rowsMock := mock.NewMockRows(ctrl)

					rowsMock.EXPECT().Next().Return(true)
					rowsMock.EXPECT().Scan(gomock.Any()).SetArg(0, testFirstPartition)

					rowsMock.EXPECT().Next()
					rowsMock.EXPECT().Err().Return(errors.New("test-error"))
					rowsMock.EXPECT().Close()

					shard.
						EXPECT().
						Query(
							gomock.Any(),
							generatedQuery,
							testDatabase,
							testTable,
							1,
						).
						Return(rowsMock, nil)
				},
				database: testDatabase,
				table:    testTable,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			shardMock := mock.NewMockShard(ctrl)

			if tt.args.prepareMock != nil {
				tt.args.prepareMock(ctrl, shardMock)
			}

			got, err := getPartitionsOnShard(context.Background(), shardMock, tt.args.database, tt.args.table)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func Test_replacePartitionsOnShard(t *testing.T) {
	t.Parallel()

	const (
		generatedQuery = `ALTER TABLE "test_database"."test_table_to" REPLACE PARTITION ? FROM "test_database"."test_table_from"`

		testDatabase  = "test_database"
		testTableFrom = "test_table_from"
		testTableTo   = "test_table_to"
		testPartition = "test-partition"
	)

	type args struct {
		prepareShardMock func(shard *mock.MockShard)
		database         string
		from             string
		to               string
		partitions       []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Ok",
			args: args{
				prepareShardMock: func(shard *mock.MockShard) {
					shard.
						EXPECT().
						Exec(
							gomock.Any(),
							generatedQuery,
							testPartition,
						).Return(nil)
				},
				database: testDatabase,
				from:     testTableFrom,
				to:       testTableTo,
				partitions: []string{
					testPartition,
				},
			},
		},
		{
			name: "Error at Exec()",
			args: args{
				prepareShardMock: func(shard *mock.MockShard) {
					shard.
						EXPECT().
						Exec(
							gomock.Any(),
							generatedQuery,
							testPartition,
						).Return(errors.New("test-error"))
				},
				database: testDatabase,
				from:     testTableFrom,
				to:       testTableTo,
				partitions: []string{
					testPartition,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			shardMock := mock.NewMockShard(ctrl)

			if tt.args.prepareShardMock != nil {
				tt.args.prepareShardMock(shardMock)
			}

			assert.Equal(
				t,
				tt.wantErr,
				replacePartitionsOnShard(context.Background(), shardMock, tt.args.database, tt.args.from, tt.args.to, tt.args.partitions) != nil,
			)
		})
	}
}
