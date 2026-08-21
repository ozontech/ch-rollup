// Copyright 2025 LLC "Ozon Technologies".
// SPDX-License-Identifier: Apache-2.0

// Package time is a utils for working with time.
package time

import "time"

// SecondsFromDuration returns duration in seconds.
func SecondsFromDuration(dur time.Duration) int {
	return int(dur / time.Second)
}

// Range represents time range with from and to.
type Range struct {
	From, To time.Time
}

// SplitTimeRangeByInterval returns split by interval time ranges.
func SplitTimeRangeByInterval(timeRange Range, interval time.Duration) []Range {
	from := timeRange.From
	to := timeRange.To

	// Protection against an infinite loop or incorrect data
	if interval <= 0 || !from.Before(to) {
		return []Range{{From: from, To: to}}
	}

	totalDist := to.Sub(from)
	if interval >= totalDist {
		return []Range{{From: from, To: to}}
	}

	// Calculate the exact number of intervals (rounded up)
	count := int((totalDist + interval - 1) / interval)
	result := make([]Range, count)

	currFrom := from
	for i := 0; i < count-1; i++ {
		currTo := currFrom.Add(interval)
		result[i] = Range{From: currFrom, To: currTo}
		currFrom = currTo
	}

	// The last interval is guaranteed to be closed using “to” (we avoid flying over)
	result[count-1] = Range{From: currFrom, To: to}

	return result
}
