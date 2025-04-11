// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package types is a general models for ch-rollup.
package types

import (
	"errors"
	"fmt"
	"time"

	sqlUtils "github.com/ozontech/ch-rollup/internal/utils/sql"
)

// Tasks ...
type Tasks []Task

// Validate Tasks.
func (t Tasks) Validate() error {
	for _, task := range t {
		if err := task.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Task ...
type Task struct {
	Database       string          // The name of the database where the table resides.
	Table          string          // The name of the table to be configured.
	PartitionKey   time.Duration   // The key used for partitioning data, typically representing a time interval.
	CopyInterval   time.Duration   // This is the interval that will be used when copying data. Default: '1h'.
	RollUpSettings []RollUpSetting // A slice of settings defining roll up intervals and specific column configurations for those intervals.
	ColumnSettings []ColumnSetting // A slice of column configuration objects that define how data is grouped and aggregated.
}

// RollUpSetting defines a specific roll up interval and the columns affected during that interval.
type RollUpSetting struct {
	After          time.Duration   // The time duration after which the roll up interval applies.
	Interval       time.Duration   // The roll up interval duration.
	ColumnSettings []ColumnSetting // A slice of column configuration objects that override the top-level column settings for the specified interval.
}

// ColumnSetting defines settings for a specific column.
type ColumnSetting struct {
	Name         string // The name of the column.
	IsRollUpTime bool   // (Optional) A boolean indicating if this column is used as the time reference for roll up.
	Expression   string // (Optional) The expression used to calculate value for the column. Example: 'countMergeState(counter)'
}

var (
	errBadPartitionKey    = errors.New("partitionKey must be greater than 0")
	errManyTimeColumns    = errors.New("only one IsRollUpTime column allowed")
	errTimeColumnNotFound = errors.New("column with IsRollUpTime not found")
)

// Validate Task.
func (t *Task) Validate() error {
	if err := sqlUtils.ValidateEntityName(t.Table); err != nil {
		return fmt.Errorf("failed to validate table name: %w", err)
	}

	if err := sqlUtils.ValidateEntityName(t.Database); err != nil {
		return fmt.Errorf("failed to validate database name: %w", err)
	}

	if t.PartitionKey <= 0 {
		return errBadPartitionKey
	}

	var rollUpTimeColumnName string

	for _, columnSetting := range t.ColumnSettings {
		if err := columnSetting.Validate(); err != nil {
			return fmt.Errorf("failed to validate column '%s': %w", columnSetting.Name, err)
		}

		if columnSetting.IsRollUpTime {
			if rollUpTimeColumnName != "" {
				return errManyTimeColumns
			}

			rollUpTimeColumnName = columnSetting.Name
		}
	}

	if rollUpTimeColumnName == "" {
		return errTimeColumnNotFound
	}

	for _, rollUpSetting := range t.RollUpSettings {
		if err := rollUpSetting.Validate(rollUpTimeColumnName); err != nil {
			return fmt.Errorf(
				"failed to validate rollUpSetting with after '%s', interval '%s': %w",
				rollUpSetting.After.String(),
				rollUpSetting.Interval.String(),
				err,
			)
		}
	}

	return nil
}

var (
	errBadAfter             = errors.New("after must be greater than 0")
	errBadInterval          = errors.New("interval must be greater than 0")
	errUnexpectedTimeColumn = errors.New("rollUpTime column can be defined only in global settings")
)

// Validate RollUpSetting.
func (rs *RollUpSetting) Validate(rollUpTimeColumnName string) error {
	if rs.After <= 0 {
		return errBadAfter
	}

	if rs.Interval <= 0 {
		return errBadInterval
	}

	for _, columnSetting := range rs.ColumnSettings {
		if err := columnSetting.Validate(); err != nil {
			return fmt.Errorf("failed to validate column '%s': %w", columnSetting.Name, err)
		}

		if columnSetting.IsRollUpTime || columnSetting.Name == rollUpTimeColumnName {
			return errUnexpectedTimeColumn
		}
	}

	return nil
}

// Validate ColumnSetting.
func (cs *ColumnSetting) Validate() error {
	if err := sqlUtils.ValidateEntityName(cs.Name); err != nil {
		return fmt.Errorf("failed to validate name: %w", err)
	}

	return nil
}
