// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvent_String(t *testing.T) {
	t.Parallel()

	type fields struct {
		Type  EventType
		Error error
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "EventTypeRollUp Ok",
			fields: fields{
				Type: EventTypeRollUp,
			},
			want: "RollUp",
		},
		{
			name: "EventTypeRollUp Error",
			fields: fields{
				Type:  EventTypeRollUp,
				Error: errors.New("test"),
			},
			want: "RollUp was failed with error: test",
		},
		{
			name:   "EventTypeEmpty Ok",
			fields: fields{},
			want:   "EventType(0)",
		},
		{
			name: "EventTypeEmpty Error",
			fields: fields{
				Error: errors.New("test"),
			},
			want: "EventType(0) was failed with error: test",
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(
				t,
				tt.want,
				Event{
					Type:  tt.fields.Type,
					Error: tt.fields.Error,
				}.String(),
			)
		})
	}
}
