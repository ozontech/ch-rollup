// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/ozontech/ch-rollup/pkg/database/mock"
)

func TestWrapNil(t *testing.T) {
	t.Parallel()

	assert.Panics(t, func() {
		Wrap(nil, nil)
	})

	ctrl := gomock.NewController(t)
	shardMock := mock.NewMockShard(ctrl)

	assert.Panics(t, func() {
		Wrap(shardMock, nil)
	})
}

type testWrapError struct{}

func (testWrapError) Error() string {
	return "test-wrapped-error"
}

func TestWrap(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	shardMock := mock.NewMockShard(ctrl)

	wrapFunc := func(err error) error {
		if err != nil {
			return testWrapError{}
		}

		return nil
	}

	wrappedShard := Wrap(shardMock, wrapFunc)

	const (
		testShardName = "test-shard"
		testQuery     = "test-query"
	)

	var (
		testError = errors.New("test-error")
	)

	t.Run("Name", func(t *testing.T) {
		t.Parallel()

		shardMock.EXPECT().Name().Return(testShardName)
		assert.Equal(t, testShardName, wrappedShard.Name())
	})

	t.Run("Exec", func(t *testing.T) {
		t.Parallel()

		shardMock.EXPECT().Exec(gomock.Any(), testQuery).Return(testError)
		assert.Equal(t, testWrapError{}, wrappedShard.Exec(context.Background(), testQuery))
	})

	t.Run("Query", func(t *testing.T) {
		t.Parallel()

		rowsMock := mock.NewMockRows(ctrl)
		rowsMock.EXPECT().Next().Return(true)
		rowsMock.EXPECT().Scan().Return(testError)
		rowsMock.EXPECT().Err().Return(testError)
		rowsMock.EXPECT().Close().Return(testError)

		shardMock.EXPECT().Query(gomock.Any(), testQuery).Return(rowsMock, testError)

		rows, err := wrappedShard.Query(context.Background(), testQuery)
		assert.True(t, rows.Next())
		assert.Equal(t, testWrapError{}, rows.Scan())
		assert.Equal(t, testWrapError{}, rows.Err())
		assert.Equal(t, testWrapError{}, rows.Close())
		assert.Equal(t, testWrapError{}, err)
	})

	t.Run("QueryRow", func(t *testing.T) {
		t.Parallel()

		rowMock := mock.NewMockRow(ctrl)
		rowMock.EXPECT().Scan().Return(testError)
		rowMock.EXPECT().Err().Return(testError)

		shardMock.EXPECT().QueryRow(gomock.Any(), testQuery).Return(rowMock)

		row := wrappedShard.QueryRow(context.Background(), testQuery)
		assert.Equal(t, testWrapError{}, row.Scan())
		assert.Equal(t, testWrapError{}, row.Err())
	})
}
