// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTask_Validate(t *testing.T) {
	t.Parallel()

	const (
		testDatabase     = "test_database"
		testTable        = "test_table"
		testPartitionKey = time.Hour * 24
	)

	var (
		testRollupSettings = []RollUpSetting{
			{
				After:    time.Hour * 24,
				Interval: time.Hour,
			},
		}

		testColumnSettings = []ColumnSetting{
			{
				Name: "test_column",
			},
			{
				Name:         "time_column",
				IsRollUpTime: true,
			},
		}
	)

	type fields struct {
		Database       string
		Table          string
		PartitionKey   time.Duration
		RollUpSettings []RollUpSetting
		ColumnSettings []ColumnSetting
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Ok",
			fields: fields{
				Database:       testDatabase,
				Table:          testTable,
				PartitionKey:   testPartitionKey,
				RollUpSettings: testRollupSettings,
				ColumnSettings: testColumnSettings,
			},
		},
		{
			name: "Bad database",
			fields: fields{
				Database:       "bad-database",
				Table:          testTable,
				PartitionKey:   testPartitionKey,
				RollUpSettings: testRollupSettings,
				ColumnSettings: testColumnSettings,
			},
			wantErr: true,
		},
		{
			name: "Bad table",
			fields: fields{
				Database:       testDatabase,
				Table:          "bad-table",
				PartitionKey:   testPartitionKey,
				RollUpSettings: testRollupSettings,
				ColumnSettings: testColumnSettings,
			},
			wantErr: true,
		},
		{
			name: "Bad partition key",
			fields: fields{
				Database:       testDatabase,
				Table:          testTable,
				PartitionKey:   -time.Hour,
				RollUpSettings: testRollupSettings,
				ColumnSettings: testColumnSettings,
			},
			wantErr: true,
		},
		{
			name: "Failed to validate column",
			fields: fields{
				Database:       testDatabase,
				Table:          testTable,
				PartitionKey:   testPartitionKey,
				RollUpSettings: testRollupSettings,
				ColumnSettings: []ColumnSetting{
					{
						Name: "bad-column",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Too many time columns",
			fields: fields{
				Database:       testDatabase,
				Table:          testTable,
				PartitionKey:   testPartitionKey,
				RollUpSettings: testRollupSettings,
				ColumnSettings: []ColumnSetting{
					{
						Name:         "first_time_column",
						IsRollUpTime: true,
					},
					{
						Name:         "second_time_column",
						IsRollUpTime: true,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Time column not found",
			fields: fields{
				Database:       testDatabase,
				Table:          testTable,
				PartitionKey:   testPartitionKey,
				RollUpSettings: testRollupSettings,
				ColumnSettings: []ColumnSetting{
					{
						Name: "column",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Failed to validate rollup settings",
			fields: fields{
				Database:     testDatabase,
				Table:        testTable,
				PartitionKey: testPartitionKey,
				RollUpSettings: []RollUpSetting{
					{},
				},
				ColumnSettings: testColumnSettings,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			task := &Task{
				Database:       tt.fields.Database,
				Table:          tt.fields.Table,
				PartitionKey:   tt.fields.PartitionKey,
				RollUpSettings: tt.fields.RollUpSettings,
				ColumnSettings: tt.fields.ColumnSettings,
			}
			assert.Equal(t, tt.wantErr, task.Validate() != nil)
		})
	}
}

func TestRollUpSetting_Validate(t *testing.T) {
	t.Parallel()

	const (
		testAfter    = time.Hour * 24
		testInterval = time.Hour
	)

	var (
		testColumnSettings = []ColumnSetting{
			{
				Name: "column",
			},
		}
	)

	type fields struct {
		After          time.Duration
		Interval       time.Duration
		ColumnSettings []ColumnSetting
	}
	tests := []struct {
		name                 string
		fields               fields
		rollUpTimeColumnName string
		wantErr              bool
	}{
		{
			name: "Ok",
			fields: fields{
				After:          testAfter,
				Interval:       testInterval,
				ColumnSettings: testColumnSettings,
			},
		},
		{
			name: "Empty after",
			fields: fields{
				After:          0,
				Interval:       testInterval,
				ColumnSettings: testColumnSettings,
			},
			wantErr: true,
		},
		{
			name: "Empty interval",
			fields: fields{
				After:          testAfter,
				Interval:       0,
				ColumnSettings: testColumnSettings,
			},
			wantErr: true,
		},
		{
			name: "Bad column",
			fields: fields{
				After:    testAfter,
				Interval: testInterval,
				ColumnSettings: []ColumnSetting{
					{},
				},
			},
			wantErr: true,
		},
		{
			name: "Unexpected time column",
			fields: fields{
				After:    testAfter,
				Interval: testInterval,
				ColumnSettings: []ColumnSetting{
					{
						Name:         "test_column",
						IsRollUpTime: true,
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rs := &RollUpSetting{
				After:          tt.fields.After,
				Interval:       tt.fields.Interval,
				ColumnSettings: tt.fields.ColumnSettings,
			}
			assert.Equal(
				t,
				tt.wantErr,
				rs.Validate(tt.rollUpTimeColumnName) != nil,
			)
		})
	}
}
