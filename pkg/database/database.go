// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package database declaring ClickHouse Cluster and Shard interface.
package database

import (
	"context"
)

//go:generate go run go.uber.org/mock/mockgen -source database.go -package=mock -destination=mock/database.go

// Cluster ...
type Cluster interface {
	// Shards returns shards for rollup.
	Shards(ctx context.Context) ([]Shard, error)
}

// Shard is an interface that declares methods that 'rollup' package use.
// You must implement QueryError for all returning errors from its methods.
type Shard interface {
	Name() string
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
}

// Rows ...
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// Row ...
type Row interface {
	Scan(dest ...any) error
	Err() error
}
