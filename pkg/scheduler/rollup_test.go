// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/ch-rollup/pkg/types"
)

func Test_prepareRollUpColumns(t *testing.T) {
	t.Parallel()

	type args struct {
		globalColumnSettings  []types.ColumnSetting
		currentColumnSettings []types.ColumnSetting
	}
	tests := []struct {
		name string
		args args
		want []types.ColumnSetting
	}{
		{
			name: "Without currentColumnSettings",
			args: args{
				globalColumnSettings: []types.ColumnSetting{
					{
						Name:       "test_column",
						Expression: "test_expression",
					},
				},
			},
			want: []types.ColumnSetting{
				{
					Name:       "test_column",
					Expression: "test_expression",
				},
			},
		},
		{
			name: "with currentColumnSettings",
			args: args{
				globalColumnSettings: []types.ColumnSetting{
					{
						Name:       "test_column",
						Expression: "global_test_expression",
					},
				},
				currentColumnSettings: []types.ColumnSetting{
					{
						Name:       "test_column",
						Expression: "current_test_expression",
					},
				},
			},
			want: []types.ColumnSetting{
				{
					Name:       "test_column",
					Expression: "current_test_expression",
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, prepareRollUpColumns(tt.args.globalColumnSettings, tt.args.currentColumnSettings))
		})
	}
}
