// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/ozontech/ch-rollup/pkg/rollup"
	"github.com/ozontech/ch-rollup/pkg/scheduler/mock"
	"github.com/ozontech/ch-rollup/pkg/types"
)

func TestNew(t *testing.T) {
	t.Parallel()

	var (
		okTasks = []types.Task{
			{
				Database:     "test_database",
				Table:        "test_table",
				PartitionKey: time.Hour,
				RollUpSettings: []types.RollUpSetting{
					{
						After:    time.Hour,
						Interval: time.Hour,
					},
				},
				ColumnSettings: []types.ColumnSetting{
					{
						Name:         "test",
						IsRollUpTime: true,
					},
				},
			},
		}
	)

	type args struct {
		tasks       types.Tasks
		prepareMock func(ctrl *gomock.Controller) RollUp
	}
	tests := []struct {
		name     string
		args     args
		wantTask []types.Task
		wantErr  bool
	}{
		{
			name: "Ok",
			args: args{
				tasks: okTasks,
				prepareMock: func(ctrl *gomock.Controller) RollUp {
					return mock.NewMockRollUp(ctrl)
				},
			},
			wantTask: okTasks,
		},
		{
			name: "Validation failed",
			args: args{
				tasks: []types.Task{
					{},
				},
				prepareMock: func(ctrl *gomock.Controller) RollUp {
					return mock.NewMockRollUp(ctrl)
				},
			},
			wantErr: true,
		},
		{
			name: "Nil rollup",
			args: args{
				tasks:       okTasks,
				prepareMock: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)

			var rollUp RollUp
			if tt.args.prepareMock != nil {
				rollUp = tt.args.prepareMock(ctrl)
			}

			got, err := New(tt.args.tasks, rollUp)
			if err == nil {
				assert.Equal(t, tt.wantTask, got.tasks)
				assert.Equal(t, rollUp, got.dbRollUp)
			}

			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestScheduler_Run_NotInitialized(t *testing.T) {
	t.Parallel()

	s := Scheduler{}
	_, err := s.Run(context.Background())
	assert.Error(t, err)
}

func TestScheduler_Run(t *testing.T) {
	t.Parallel()

	/*
		Current we test only first rollup, because next schedule will be after defaultSchedulerInterval.
	*/

	type fields struct {
		tasks             []types.Task
		prepareRollUpMock func(rollUp *mock.MockRollUp)
	}
	tests := []struct {
		name   string
		fields fields
		want   []Event
	}{
		{
			name: "Ok",
			fields: fields{
				tasks: []types.Task{
					{
						Database:     "test_database",
						Table:        "test_table",
						PartitionKey: time.Hour * 24,
						CopyInterval: time.Hour,
						RollUpSettings: []types.RollUpSetting{
							{
								After:    time.Hour * 24,
								Interval: time.Hour,
							},
						},
						ColumnSettings: []types.ColumnSetting{
							{
								Name:         "test_interval",
								IsRollUpTime: true,
							},
						},
					},
				},
				prepareRollUpMock: func(rollUp *mock.MockRollUp) {
					rollUp.EXPECT().Run(
						gomock.Any(),
						rollup.RunOptions{
							Database:     "test_database",
							Table:        "test_table",
							TempTable:    "test_table_temp",
							PartitionKey: time.Hour * 24,
							Columns: []types.ColumnSetting{
								{
									Name:         "test_interval",
									IsRollUpTime: true,
								},
							},
							After:        time.Hour * 24,
							Interval:     time.Hour,
							CopyInterval: time.Hour,
						},
					).Return(nil)
				},
			},
			want: []Event{
				{
					Type: EventTypeRollUp,
				},
			},
		},
		{
			name: "With error",
			fields: fields{
				tasks: []types.Task{
					{
						Database:     "test_database",
						Table:        "test_table",
						PartitionKey: time.Hour * 24,
						CopyInterval: time.Hour,
						RollUpSettings: []types.RollUpSetting{
							{
								After:    time.Hour * 24,
								Interval: time.Hour,
							},
						},
						ColumnSettings: []types.ColumnSetting{
							{
								Name:         "test_interval",
								IsRollUpTime: true,
							},
						},
					},
				},
				prepareRollUpMock: func(rollUp *mock.MockRollUp) {
					rollUp.EXPECT().Run(
						gomock.Any(),
						rollup.RunOptions{
							Database:     "test_database",
							Table:        "test_table",
							TempTable:    "test_table_temp",
							PartitionKey: time.Hour * 24,
							Columns: []types.ColumnSetting{
								{
									Name:         "test_interval",
									IsRollUpTime: true,
								},
							},
							After:        time.Hour * 24,
							Interval:     time.Hour,
							CopyInterval: time.Hour,
						},
					).Return(errors.New("test-error"))
				},
			},
			want: []Event{
				{
					Type:  EventTypeRollUp,
					Error: errors.New("test-error"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)

			rollUpMock := mock.NewMockRollUp(ctrl)

			if tt.fields.prepareRollUpMock != nil {
				tt.fields.prepareRollUpMock(rollUpMock)
			}

			s := &Scheduler{
				tasks:    tt.fields.tasks,
				dbRollUp: rollUpMock,
			}

			// Second must be enough to process first rollup.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			eventsChan, _ := s.Run(ctx)

			var gotEvents []Event

			for event := range eventsChan {
				gotEvents = append(gotEvents, event)
			}

			assert.Equal(t, tt.want, gotEvents)
		})
	}
}
