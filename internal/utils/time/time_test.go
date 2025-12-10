// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package time //revive:disable-line:var-naming

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSecondsFromDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dur  time.Duration
		want int
	}{
		{
			name: "Seconds",
			dur:  time.Second * 5,
			want: 5,
		},
		{
			name: "Minutes",
			dur:  time.Minute * 3,
			want: 180,
		},
		{
			name: "Hours",
			dur:  time.Hour * 2,
			want: 7200,
		},
		{
			name: "0",
			dur:  0,
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, SecondsFromDuration(tt.dur))
		})
	}
}

func TestSplitTimeRangeByInterval(t *testing.T) {
	t.Parallel()

	now := time.Now()

	type args struct {
		timeRange Range
		interval  time.Duration
	}
	tests := []struct {
		name string
		args args
		want []Range
	}{
		{
			name: "Less than the interval",
			args: args{
				timeRange: Range{
					From: now,
					To:   now.Add(time.Minute * 30),
				},
				interval: time.Hour,
			},
			want: []Range{
				{
					From: now,
					To:   now.Add(time.Minute * 30),
				},
			},
		},
		{
			name: "Equal to interval",
			args: args{
				timeRange: Range{
					From: now,
					To:   now.Add(time.Hour),
				},
				interval: time.Hour,
			},
			want: []Range{
				{
					From: now,
					To:   now.Add(time.Hour),
				},
			},
		},
		{
			name: "Equal to interval ranges",
			args: args{
				timeRange: Range{
					From: now,
					To:   now.Add(time.Hour * 3),
				},
				interval: time.Hour,
			},
			want: []Range{
				{
					From: now,
					To:   now.Add(time.Hour),
				},
				{
					From: now.Add(time.Hour),
					To:   now.Add(time.Hour * 2),
				},
				{
					From: now.Add(time.Hour * 2),
					To:   now.Add(time.Hour * 3),
				},
			},
		},
		{
			name: "Greater then interval ranges",
			args: args{
				timeRange: Range{
					From: now,
					To:   now.Add(time.Hour*2 + time.Minute*30),
				},
				interval: time.Hour,
			},
			want: []Range{
				{
					From: now,
					To:   now.Add(time.Hour),
				},
				{
					From: now.Add(time.Hour),
					To:   now.Add(time.Hour * 2),
				},
				{
					From: now.Add(time.Hour * 2),
					To:   now.Add(time.Hour*2 + time.Minute*30),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, SplitTimeRangeByInterval(tt.args.timeRange, tt.args.interval))
		})
	}
}
