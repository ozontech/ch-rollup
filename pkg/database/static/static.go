// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package static implements connect to static Clickhouse node or cluster.
package static

import (
	"context"
	"errors"

	"go.uber.org/multierr"

	"github.com/ozontech/ch-rollup/pkg/database"
	clickhouseGoWrap "github.com/ozontech/ch-rollup/pkg/database/error_wrappers/clickhouse_go"
)

// Cluster ...
type Cluster struct {
	shards []shard
}

// NewOptions ...
type NewOptions struct {
	Address     string
	Username    string
	Password    string
	ClusterName string
}

func (opts NewOptions) validate() error {
	if len(opts.Address) == 0 {
		return errors.New("address is required")
	}

	if len(opts.Username) == 0 {
		return errors.New("username is required")
	}

	return nil
}

// New returns new static Cluster.
func New(ctx context.Context, opts NewOptions) (*Cluster, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	conn, err := connect(ctx, opts.Address, opts.Username, opts.Password)
	if err != nil {
		return nil, err
	}

	if opts.ClusterName == "" {
		return &Cluster{
			shards: []shard{
				{
					name: "main",
					conn: conn,
				},
			},
		}, nil
	}

	shards, err := openClusterShards(ctx, conn, opts.Username, opts.Password, opts.ClusterName)
	if err != nil {
		return nil, err
	}

	return &Cluster{
		shards: shards,
	}, nil
}

// Shards returns slice of Shards.
func (c *Cluster) Shards(_ context.Context) ([]database.Shard, error) {
	result := make([]database.Shard, 0, len(c.shards))

	for _, shard := range c.shards {
		result = append(result, clickhouseGoWrap.Wrap(&shard))
	}

	return result, nil
}

// Close Cluster.
func (c *Cluster) Close() (err error) {
	for _, shard := range c.shards {
		err = multierr.Append(err, shard.Close())
	}

	return err
}
