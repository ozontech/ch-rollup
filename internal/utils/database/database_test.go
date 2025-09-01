// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package database

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	mockDatabase "github.com/ozontech/ch-rollup/pkg/database/mock"
)

func TestCreateTableAs(t *testing.T) {
	t.Parallel()

	const (
		testDatabaseName = "test_database"
		testSrcTableName = "test_src_table"
		testDstTableName = "test_dst_table"
	)

	type args struct {
		database string
		srcTable string
		dstTable string
	}
	tests := []struct {
		name             string
		prepareShardMock func(mockShard *mockDatabase.MockShard)
		args             args
		wantErr          bool
	}{
		{
			name: "Ok",
			prepareShardMock: func(mockShard *mockDatabase.MockShard) {
				mockShard.
					EXPECT().
					Exec(
						gomock.Any(),
						`CREATE TABLE "test_database"."test_dst_table" AS "test_database"."test_src_table"`,
					).
					Return(nil)
			},
			args: args{
				database: testDatabaseName,
				srcTable: testSrcTableName,
				dstTable: testDstTableName,
			},
		},
		{
			name: "Error InvalidArguments",
			args: args{
				database: "$",
				srcTable: "&",
				dstTable: "123",
			},
			wantErr: true,
		},
		{
			name: "Error at Exec",
			prepareShardMock: func(mockShard *mockDatabase.MockShard) {
				mockShard.
					EXPECT().
					Exec(
						gomock.Any(),
						`CREATE TABLE "test_database"."test_dst_table" AS "test_database"."test_src_table"`,
					).
					Return(errors.New("test error"))
			},
			args: args{
				database: testDatabaseName,
				srcTable: testSrcTableName,
				dstTable: testDstTableName,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			shardMock := mockDatabase.NewMockShard(ctrl)

			if tt.prepareShardMock != nil {
				tt.prepareShardMock(shardMock)
			}

			err := CreateTableAs(context.Background(), shardMock, tt.args.database, tt.args.srcTable, tt.args.dstTable)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestDropTable(t *testing.T) {
	t.Parallel()

	const (
		testDatabaseName = "test_database"
		testTableName    = "test_table"
	)

	type args struct {
		database string
		table    string
	}
	tests := []struct {
		name             string
		prepareShardMock func(mockShard *mockDatabase.MockShard)
		args             args
		wantErr          bool
	}{
		{
			name: "Ok",
			prepareShardMock: func(mockShard *mockDatabase.MockShard) {
				mockShard.
					EXPECT().
					Exec(
						gomock.Any(),
						`DROP TABLE "test_database"."test_table"`,
					).
					Return(nil)
			},
			args: args{
				database: testDatabaseName,
				table:    testTableName,
			},
		},
		{
			name: "Error InvalidArguments",
			args: args{
				database: "$",
				table:    "123",
			},
			wantErr: true,
		},
		{
			name: "Error at Exec",
			prepareShardMock: func(mockShard *mockDatabase.MockShard) {
				mockShard.
					EXPECT().
					Exec(
						gomock.Any(),
						`DROP TABLE "test_database"."test_table"`,
					).
					Return(errors.New("test error"))
			},
			args: args{
				database: testDatabaseName,
				table:    testTableName,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			shardMock := mockDatabase.NewMockShard(ctrl)

			if tt.prepareShardMock != nil {
				tt.prepareShardMock(shardMock)
			}

			err := DropTable(context.Background(), shardMock, tt.args.database, tt.args.table)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}
