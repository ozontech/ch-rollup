// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package database

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  QueryError
		want string
	}{
		{
			name: "UnknownTable",
			err: QueryError{
				Type: ErrUnknownTable,
			},
			want: "UnknownTable",
		},
		{
			name: "TableAlreadyExists",
			err: QueryError{
				Type: ErrTableAlreadyExists,
			},
			want: "TableAlreadyExists",
		},
		{
			name: "Unknown",
			err:  QueryError{},
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.err.Error())
		})
	}
}

func TestQueryError_Unwrap(t *testing.T) {
	t.Parallel()

	testErr := errors.New("test error")
	queryErr := QueryError{
		Inner: testErr,
	}

	assert.Equal(t, true, errors.Is(queryErr, testErr))
}
