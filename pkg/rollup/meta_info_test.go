// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package rollup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/ozontech/ch-rollup/pkg/database/mock"
)

func Test_getLatestRollUpByKeyOnShard(t *testing.T) {
	t.Parallel()

	var (
		testTime = time.Now()
	)

	type args struct {
		prepareMock func(ctrl *gomock.Controller, shard *mock.MockShard)
		key         metaInfoKey
	}
	tests := []struct {
		name    string
		args    args
		want    time.Time
		wantErr bool
	}{
		{
			name: "Ok",
			args: args{
				prepareMock: func(ctrl *gomock.Controller, shard *mock.MockShard) {
					rowMock := mock.NewMockRow(ctrl)

					rowMock.EXPECT().Scan(gomock.Any()).SetArg(0, testTime).Return(nil)

					shard.
						EXPECT().
						QueryRow(
							gomock.Any(),
							"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
							"test_database",
							"test_table",
							3600,
							3600,
						).
						Return(rowMock)
				},
				key: metaInfoKey{
					Database: "test_database",
					Table:    "test_table",
					After:    time.Hour,
					Interval: time.Hour,
				},
			},
			want: testTime,
		},
		{
			name: "Error",
			args: args{
				prepareMock: func(ctrl *gomock.Controller, shard *mock.MockShard) {
					rowMock := mock.NewMockRow(ctrl)

					rowMock.EXPECT().Scan(gomock.Any()).Return(errors.New("test-error"))

					shard.
						EXPECT().
						QueryRow(
							gomock.Any(),
							"SELECT max(roll_ups_at) FROM rollup_meta_info WHERE database = ? AND table = ? AND after_sec = ? AND interval_sec = ? GROUP BY database, table, after_sec, interval_sec",
							"test_database",
							"test_table",
							3600,
							3600,
						).
						Return(rowMock)
				},
				key: metaInfoKey{
					Database: "test_database",
					Table:    "test_table",
					After:    time.Hour,
					Interval: time.Hour,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			shardMock := mock.NewMockShard(ctrl)

			if tt.args.prepareMock != nil {
				tt.args.prepareMock(ctrl, shardMock)
			}

			got, err := getLatestRollUpByKeyOnShard(context.Background(), shardMock, tt.args.key)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func Test_addMetaInfoOnShard(t *testing.T) {
	t.Parallel()

	var (
		testTime = time.Now()
	)

	type args struct {
		prepareShardMock func(shard *mock.MockShard)
		metaInfo         metaInfo
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
							"INSERT INTO rollup_meta_info (database, table, after_sec, interval_sec, roll_ups_at) VALUES (?, ?, ?, ?, ?)",
							"test_database",
							"test_table",
							3600,
							3600,
							testTime,
						).
						Return(nil)
				},
				metaInfo: metaInfo{
					Database:  "test_database",
					Table:     "test_table",
					After:     time.Hour,
					Interval:  time.Hour,
					RollUpsAt: testTime,
				},
			},
		},
		{
			name: "Error",
			args: args{
				prepareShardMock: func(shard *mock.MockShard) {
					shard.
						EXPECT().
						Exec(
							gomock.Any(),
							"INSERT INTO rollup_meta_info (database, table, after_sec, interval_sec, roll_ups_at) VALUES (?, ?, ?, ?, ?)",
							"test_database",
							"test_table",
							3600,
							3600,
							testTime,
						).
						Return(errors.New("test-error"))
				},
				metaInfo: metaInfo{
					Database:  "test_database",
					Table:     "test_table",
					After:     time.Hour,
					Interval:  time.Hour,
					RollUpsAt: testTime,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
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
				addMetaInfoOnShard(context.Background(), shardMock, tt.args.metaInfo) != nil,
			)
		})
	}
}
