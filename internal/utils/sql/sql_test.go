// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateEntityName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		s       string
		wantErr bool
	}{
		{
			name: "Ok",
			s:    "test_table",
		},
		{
			name:    "Empty",
			s:       "",
			wantErr: true,
		},
		{
			name:    "Bad",
			s:       "bad!table",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(
				t,
				tt.wantErr,
				ValidateEntityName(tt.s) != nil,
			)
		})
	}
}

func TestQuotedDatabaseEntity(t *testing.T) {
	t.Parallel()

	assert.Equal(t, `"test-database"."test-table"`, QuotedDatabaseEntity("test-database", "test-table"))
}

func TestQuotedEntity(t *testing.T) {
	t.Parallel()

	assert.Equal(t, `"test-entity"`, QuotedEntity("test-entity"))
}
