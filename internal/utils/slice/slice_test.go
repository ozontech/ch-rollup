// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

package slice

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertFuncWithSkip(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		slice     []int
		want      []string
		skipElems map[int]struct{}
	}
	tests := []testCase{
		{
			name: "Simple",
			slice: []int{
				1, 2, 3,
			},
			want: []string{
				"1", "3",
			},
			skipElems: map[int]struct{}{
				2: {},
			},
		},
		{
			name:  "Nil",
			slice: nil,
			want:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(
				t,
				tt.want,
				ConvertFuncWithSkip(tt.slice,
					func(elem int) (string, bool) {
						_, skip := tt.skipElems[elem]
						return strconv.Itoa(elem), skip
					},
				),
			)
		})
	}
}

func TestConvertFunc(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		slice []int
		want  []string
	}
	tests := []testCase{
		{
			name: "Simple",
			slice: []int{
				1, 2, 3,
			},
			want: []string{
				"1", "2", "3",
			},
		},
		{
			name:  "Nil",
			slice: nil,
			want:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(
				t,
				tt.want,
				ConvertFunc(tt.slice,
					func(elem int) string {
						return strconv.Itoa(elem)
					},
				),
			)
		})
	}
}
