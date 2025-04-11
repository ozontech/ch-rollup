// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package common implements common error wrapper, that can be used by others.
package common

import (
	"context"

	"github.com/ozontech/ch-rollup/pkg/database"
)

type wrapErrorFunc func(err error) error

type shardWrap struct {
	shard     database.Shard
	wrapError wrapErrorFunc
}

// Wrap is a common wrap method that wraps errors and can be used at another wrapper.
// wrapError and shard must be not nil, panics if nil.
func Wrap(shard database.Shard, wrapError wrapErrorFunc) database.Shard {
	if shard == nil {
		panic("cannot wrap with nil shard")
	}

	if wrapError == nil {
		panic("cannot wrap with nil wrapError")
	}

	return &shardWrap{
		shard:     shard,
		wrapError: wrapError,
	}
}

func (w *shardWrap) Name() string {
	return w.shard.Name()
}

func (w *shardWrap) Exec(ctx context.Context, query string, args ...any) error {
	return w.wrapError(w.shard.Exec(ctx, query, args...))
}

func (w *shardWrap) Query(ctx context.Context, query string, args ...any) (database.Rows, error) {
	rows, err := w.shard.Query(ctx, query, args...)
	return newRowsWrap(rows, err, w.wrapError)
}

func (w *shardWrap) QueryRow(ctx context.Context, query string, args ...any) database.Row {
	return newRowWrap(w.shard.QueryRow(ctx, query, args...), w.wrapError)
}

func newRowWrap(row database.Row, wrapError wrapErrorFunc) database.Row {
	return &rowWrap{
		row:       row,
		wrapError: wrapError,
	}
}

func newRowsWrap(rows database.Rows, err error, wrapError wrapErrorFunc) (database.Rows, error) {
	return &rowsWrap{
		rows:      rows,
		wrapError: wrapError,
	}, wrapError(err)
}

type rowsWrap struct {
	rows      database.Rows
	wrapError wrapErrorFunc
}

func (r *rowsWrap) Next() bool {
	return r.rows.Next()
}

func (r *rowsWrap) Scan(dest ...any) error {
	return r.wrapError(r.rows.Scan(dest...))
}

func (r *rowsWrap) Close() error {
	return r.wrapError(r.rows.Close())
}

func (r *rowsWrap) Err() error {
	return r.wrapError(r.rows.Err())
}

type rowWrap struct {
	row       database.Row
	wrapError wrapErrorFunc
}

func (r *rowWrap) Err() error {
	return r.wrapError(r.row.Err())
}

func (r *rowWrap) Scan(dest ...any) error {
	return r.wrapError(r.row.Scan(dest...))
}
