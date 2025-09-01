// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package rollup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ozontech/ch-rollup/pkg/types"
)

func Test_generateRollUpStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts generateRollUpStatementOptions
		want string
	}{
		{
			name: "Ok",
			opts: generateRollUpStatementOptions{
				Database:  "test_database",
				FromTable: "test_from_table",
				ToTable:   "test_to_table",
				Interval:  time.Hour,
				Columns: []types.ColumnSetting{
					{
						Name: "first",
					},
					{
						Name:       "second",
						Expression: "max(second)",
					},
					{
						Name: "third",
					},
					{
						Name:         "rollup_time",
						IsRollUpTime: true,
					},
				},
			},
			want: `INSERT INTO "test_database"."test_to_table" ("first", "second", "third", "rollup_time") SELECT "first", max(second), "third", toStartOfInterval("rollup_time", INTERVAL 3600 SECOND) as "rollup_time" FROM "test_database"."test_from_table" WHERE "test_from_table"."rollup_time" >= ? AND "test_from_table"."rollup_time" < ? GROUP BY "first", "third", "rollup_time"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, generateRollUpStatement(tt.opts))
		})
	}
}
