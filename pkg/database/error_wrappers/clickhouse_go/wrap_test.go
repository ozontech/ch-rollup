// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package clickhouse_go

import (
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/ozontech/ch-rollup/pkg/database"
	"github.com/ozontech/ch-rollup/pkg/database/mock"
)

func TestWrap(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	shardMock := mock.NewMockShard(ctrl)

	assert.NotNil(t, Wrap(shardMock))
}

func Test_wrapException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   error
		wantErr error
	}{
		{
			name: "Unknown table",
			input: &proto.Exception{
				Code: unknownTableExceptionCode,
			},
			wantErr: database.QueryError{
				Type: database.ErrUnknownTable,
				Inner: &proto.Exception{
					Code: unknownTableExceptionCode,
				},
			},
		},
		{
			name: "Table already exists",
			input: &proto.Exception{
				Code: tableAlreadyExistsCode,
			},
			wantErr: database.QueryError{
				Type: database.ErrTableAlreadyExists,
				Inner: &proto.Exception{
					Code: tableAlreadyExistsCode,
				},
			},
		},
		{
			name:    "proto.Exception with code 0",
			input:   &proto.Exception{},
			wantErr: &proto.Exception{},
		},
		{
			name:    "Nil",
			input:   nil,
			wantErr: nil,
		},
		{
			name:    "Not proto.Exception",
			input:   errors.New("test-error"),
			wantErr: errors.New("test-error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantErr, wrapException(tt.input))
		})
	}
}
