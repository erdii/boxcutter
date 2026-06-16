package contention

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snap     Snapshot
		expected time.Duration
	}{
		{
			name:     "no conflicts",
			snap:     Snapshot{ConflictCount: 0},
			expected: 0,
		},
		{
			name:     "one conflict",
			snap:     Snapshot{ConflictCount: 1},
			expected: 5 * time.Second,
		},
		{
			name:     "two conflicts",
			snap:     Snapshot{ConflictCount: 2},
			expected: 5 * time.Second,
		},
		{
			name:     "three conflicts",
			snap:     Snapshot{ConflictCount: 3},
			expected: 30 * time.Second,
		},
		{
			name:     "five conflicts",
			snap:     Snapshot{ConflictCount: 5},
			expected: 30 * time.Second,
		},
		{
			name:     "six conflicts",
			snap:     Snapshot{ConflictCount: 6},
			expected: 1 * time.Minute,
		},
		{
			name:     "ten conflicts",
			snap:     Snapshot{ConflictCount: 10},
			expected: 1 * time.Minute,
		},
		{
			name:     "eleven conflicts",
			snap:     Snapshot{ConflictCount: 11},
			expected: 2 * time.Minute,
		},
		{
			name: "many managers doubles backoff",
			snap: Snapshot{
				ConflictCount:  3,
				UniqueManagers: map[string]int{"a": 1, "b": 1, "c": 1},
			},
			expected: 1 * time.Minute,
		},
		{
			name: "two managers does not double",
			snap: Snapshot{
				ConflictCount:  3,
				UniqueManagers: map[string]int{"a": 1, "b": 2},
			},
			expected: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := DefaultBackoff(tt.snap)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBackoffConfig_Custom(t *testing.T) {
	t.Parallel()

	cfg := BackoffConfig{
		Steps: []BackoffStep{
			{Threshold: 1, Duration: 1 * time.Second},
			{Threshold: 5, Duration: 10 * time.Second},
		},
		ManagerMultiplierThreshold: 2,
		ManagerMultiplier:          3,
	}
	fn := cfg.BackoffFunc()

	t.Run("no conflicts", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, time.Duration(0), fn(Snapshot{ConflictCount: 0}))
	})

	t.Run("below first step", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, time.Duration(0), fn(Snapshot{}))
	})

	t.Run("first step", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 1*time.Second, fn(Snapshot{ConflictCount: 1}))
	})

	t.Run("between steps", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 1*time.Second, fn(Snapshot{ConflictCount: 3}))
	})

	t.Run("second step", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 10*time.Second, fn(Snapshot{ConflictCount: 5}))
	})

	t.Run("above all steps", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 10*time.Second, fn(Snapshot{ConflictCount: 100}))
	})

	t.Run("manager multiplier kicks in", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 30*time.Second, fn(Snapshot{
			ConflictCount:  5,
			UniqueManagers: map[string]int{"a": 1, "b": 1},
		}))
	})

	t.Run("manager multiplier below threshold", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 10*time.Second, fn(Snapshot{
			ConflictCount:  5,
			UniqueManagers: map[string]int{"a": 1},
		}))
	})
}

func TestBackoffConfig_DisabledMultiplier(t *testing.T) {
	t.Parallel()

	cfg := BackoffConfig{
		Steps: []BackoffStep{
			{Threshold: 1, Duration: 5 * time.Second},
		},
		ManagerMultiplierThreshold: 0,
	}
	fn := cfg.BackoffFunc()

	assert.Equal(t, 5*time.Second, fn(Snapshot{
		ConflictCount:  1,
		UniqueManagers: map[string]int{"a": 1, "b": 1, "c": 1},
	}))
}

func TestDefaultBackoffConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultBackoffConfig()
	assert.Len(t, cfg.Steps, 4)
	assert.Equal(t, 3, cfg.ManagerMultiplierThreshold)
	assert.Equal(t, 2, cfg.ManagerMultiplier)
}
