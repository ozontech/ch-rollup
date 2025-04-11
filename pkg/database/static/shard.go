// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package static

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/ozontech/ch-rollup/pkg/database"
)

type shard struct {
	name string
	conn clickhouse.Conn
}

func (c *shard) Name() string {
	return c.name
}

func (c *shard) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c *shard) Query(ctx context.Context, query string, args ...any) (database.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

func (c *shard) QueryRow(ctx context.Context, query string, args ...any) database.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

func (c *shard) Close() error {
	return c.conn.Close()
}
