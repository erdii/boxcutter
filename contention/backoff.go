package contention

import (
	"cmp"
	"slices"
	"time"
)

// BackoffFunc computes a backoff duration from a Snapshot.
type BackoffFunc func(snap Snapshot) time.Duration

// BackoffStep maps a conflict count threshold to a backoff duration.
// The step applies when ConflictCount >= Threshold and no higher
// threshold matches.
type BackoffStep struct {
	Threshold int
	Duration  time.Duration
}

// BackoffConfig holds the tunable parameters for the escalating backoff.
type BackoffConfig struct {
	// Steps defines the escalating backoff tiers. Each step applies when
	// ConflictCount >= Threshold and no higher step matches.
	// Steps are sorted by ascending Threshold when BackoffFunc is called.
	// When empty, backoff is always 0.
	Steps []BackoffStep
	// ManagerMultiplierThreshold is the minimum number of unique
	// competing managers that triggers the multiplier. 0 disables.
	ManagerMultiplierThreshold int
	// ManagerMultiplier is applied to the backoff when the number of
	// unique managers meets or exceeds ManagerMultiplierThreshold.
	ManagerMultiplier int
}

// DefaultBackoffConfig returns the default escalating backoff configuration.
func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		Steps: []BackoffStep{
			{Threshold: 1, Duration: 5 * time.Second},
			{Threshold: 3, Duration: 30 * time.Second},
			{Threshold: 6, Duration: 1 * time.Minute},
			{Threshold: 11, Duration: 2 * time.Minute},
		},
		ManagerMultiplierThreshold: 3,
		ManagerMultiplier:          2,
	}
}

// BackoffFunc returns a BackoffFunc that uses this configuration.
// Steps are sorted by ascending Threshold on construction.
func (cfg BackoffConfig) BackoffFunc() BackoffFunc {
	sorted := slices.Clone(cfg.Steps)
	slices.SortFunc(sorted, func(a, b BackoffStep) int {
		return cmp.Compare(a.Threshold, b.Threshold)
	})

	multiplierThreshold := cfg.ManagerMultiplierThreshold
	multiplier := cfg.ManagerMultiplier

	return func(snap Snapshot) time.Duration {
		if snap.ConflictCount == 0 || len(sorted) == 0 {
			return 0
		}

		var backoff time.Duration

		for _, step := range sorted {
			if snap.ConflictCount >= step.Threshold {
				backoff = step.Duration
			}
		}

		if multiplierThreshold > 0 &&
			len(snap.UniqueManagers) >= multiplierThreshold &&
			multiplier > 0 {
			backoff *= time.Duration(multiplier)
		}

		return backoff
	}
}

var defaultBackoffFunc = DefaultBackoffConfig().BackoffFunc()

// DefaultBackoff implements an escalating backoff strategy based on
// conflict count and the number of competing managers.
func DefaultBackoff(snap Snapshot) time.Duration {
	return defaultBackoffFunc(snap)
}
