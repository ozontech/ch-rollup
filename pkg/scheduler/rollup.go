// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"context"
	"maps"
	"slices"

	"github.com/ozontech/ch-rollup/pkg/rollup"
	"github.com/ozontech/ch-rollup/pkg/types"
)

const (
	tempTablePrefix = "_temp"
)

func (s *Scheduler) rollUp(ctx context.Context) error {
	for _, task := range s.tasks {
		for _, rollUpSetting := range task.RollUpSettings {
			err := s.dbRollUp.Run(ctx, rollup.RunOptions{
				Database:     task.Database,
				Table:        task.Table,
				TempTable:    task.Table + tempTablePrefix,
				PartitionKey: task.PartitionKey,
				Columns:      prepareRollUpColumns(task.ColumnSettings, rollUpSetting.ColumnSettings),
				Interval:     rollUpSetting.Interval,
				After:        rollUpSetting.After,
				CopyInterval: task.CopyInterval,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func prepareRollUpColumns(globalColumnSettings, currentColumnSettings []types.ColumnSetting) []types.ColumnSetting {
	result := make(map[string]types.ColumnSetting, len(globalColumnSettings)+len(currentColumnSettings))

	for _, columnSettings := range globalColumnSettings {
		result[columnSettings.Name] = columnSettings
	}

	for _, columnSettings := range currentColumnSettings {
		result[columnSettings.Name] = columnSettings
	}

	return slices.Collect(maps.Values(result))
}
