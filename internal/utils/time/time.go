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
	// TODO: refac

	from := timeRange.From
	to := timeRange.To

	if interval >= to.Sub(from) {
		return []Range{
			{
				From: from,
				To:   to,
			},
		}
	}

	var result []Range

	next := from

	for next.Before(to) {
		curFrom := next
		curNext := next.Add(interval)
		if curNext.After(to) {
			result = append(result, Range{
				From: curFrom,
				To:   to,
			})

			break
		}

		next = curNext

		result = append(result, Range{
			From: curFrom,
			To:   curNext,
		})
	}

	return result
}
