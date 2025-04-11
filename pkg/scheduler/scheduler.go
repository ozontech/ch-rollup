// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package scheduler implements ch-rollup scheduler.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ozontech/ch-rollup/pkg/rollup"
	"github.com/ozontech/ch-rollup/pkg/types"
)

//go:generate go run go.uber.org/mock/mockgen -source scheduler.go -package=mock -destination=mock/scheduler.go

// RollUp ...
type RollUp interface {
	Run(ctx context.Context, opts rollup.RunOptions) error
}

const (
	defaultSchedulerInterval = time.Hour
)

// Scheduler of ch-rollup.
type Scheduler struct {
	tasks    []types.Task
	dbRollUp RollUp
}

var (
	errNewNilRollup = errors.New("rollUp must be not nil")
)

// New returns new Scheduler.
func New(tasks types.Tasks, rollUp RollUp) (*Scheduler, error) {
	if err := tasks.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate tasks: %w", err)
	}

	if rollUp == nil {
		return nil, errNewNilRollup
	}

	return &Scheduler{
		tasks:    tasks,
		dbRollUp: rollUp,
	}, nil
}

var (
	errSchedulerNotInitialized = errors.New("scheduler not initialized")
)

// Run Scheduler.
func (s *Scheduler) Run(ctx context.Context) (<-chan Event, error) {
	if s == nil || s.dbRollUp == nil {
		return nil, errSchedulerNotInitialized
	}

	eventChan := make(chan Event)

	go func() {
		defer close(eventChan)

		// Let's do first rollup immediately.
		eventChan <- Event{
			Type:  EventTypeRollUp,
			Error: s.rollUp(ctx),
		}

		ticker := time.NewTicker(defaultSchedulerInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				eventChan <- Event{
					Type:  EventTypeRollUp,
					Error: s.rollUp(ctx),
				}

				ticker.Reset(defaultSchedulerInterval)
			case <-ctx.Done():
				return
			}
		}
	}()

	return eventChan, nil
}
