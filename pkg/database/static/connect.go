// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package static

import (
	"context"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/multierr"
)

func connect(ctx context.Context, address, userName, password string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Username: userName,
			Password: password,
		},
	})
	if err != nil {
		return nil, err
	}

	return conn, conn.Ping(ctx)
}

func openClusterShards(ctx context.Context, conn clickhouse.Conn, userName, password, clusterName string) ([]shard, error) {
	shardsAddresses, err := getShardsAddresses(ctx, conn, clusterName)
	if err != nil {
		return nil, err
	}

	shards := make([]shard, 0, len(shardsAddresses))

	for _, shardAddress := range shardsAddresses {
		conn, err = connect(ctx, shardAddress, userName, password)
		if err != nil {
			return nil, err
		}

		shards = append(
			shards,
			shard{
				name: shardAddress,
				conn: conn,
			},
		)
	}

	return shards, nil
}

func getShardsAddresses(ctx context.Context, conn clickhouse.Conn, clusterName string) ([]string, error) {
	// TODO: handle replicas
	rows, err := conn.Query(ctx, `
		SELECT 
			host_address, port
		FROM 
		    system.clusters
		WHERE 
		    cluster = $1
	`, clusterName)
	if err != nil {
		return nil, err
	}

	defer func() {
		if rowsCloseErr := rows.Close(); rowsCloseErr != nil {
			err = multierr.Append(err, rowsCloseErr)
		}
	}()

	var result []string

	for rows.Next() {
		var host string
		var port int

		result = append(result, host+":"+strconv.Itoa(port))
	}

	return result, nil
}
