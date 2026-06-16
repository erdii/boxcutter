package contention

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"pkg.package-operator.run/boxcutter/machinery/types"
)

func testRef(name string) types.ObjectRef {
	return types.ObjectRef{
		GroupVersionKind: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
		ObjectKey:        client.ObjectKey{Namespace: "default", Name: name},
	}
}

func TestNewSlidingWindow_Defaults(t *testing.T) {
	t.Parallel()

	sw := NewSlidingWindow(SlidingWindowConfig{})
	assert.NotNil(t, sw.config.BackoffFunc)
	assert.NotNil(t, sw.config.Clock)
	assert.Equal(t, defaultWindow, sw.config.Window)
}

func TestSlidingWindow_RecordReconcile(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})

	ref := testRef("test-cm")
	sw.RecordReconcile(ref, types.ReconcileOutcome{
		Action:           "Updated",
		ConflictDetected: false,
	})

	snap := sw.Snapshot(ref)
	assert.Equal(t, 1, snap.ReconcileCount)
	assert.Equal(t, 0, snap.ConflictCount)
	assert.Equal(t, 0, snap.TeardownCount)
}

func TestSlidingWindow_RecordTeardown(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})

	ref := testRef("test-cm")
	sw.RecordTeardown(ref, true)

	snap := sw.Snapshot(ref)
	assert.Equal(t, 0, snap.ReconcileCount)
	assert.Equal(t, 1, snap.TeardownCount)
}

func TestSlidingWindow_ConflictTracking(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})

	ref := testRef("test-cm")
	sw.RecordReconcile(ref, types.ReconcileOutcome{
		Action:              "Recovered",
		ConflictDetected:    true,
		ConflictingManagers: []string{"other-controller"},
	})
	sw.RecordReconcile(ref, types.ReconcileOutcome{
		Action:              "Collision",
		ConflictDetected:    true,
		ConflictingManagers: []string{"another-controller"},
	})
	sw.RecordReconcile(ref, types.ReconcileOutcome{
		Action: "Idle",
	})

	snap := sw.Snapshot(ref)
	assert.Equal(t, 3, snap.ReconcileCount)
	assert.Equal(t, 2, snap.ConflictCount)
	assert.Equal(t, 1, snap.RecoveryCount)
	assert.Equal(t, 1, snap.CollisionCount)
	require.Len(t, snap.UniqueManagers, 2)
	assert.Equal(t, 1, snap.UniqueManagers["other-controller"])
	assert.Equal(t, 1, snap.UniqueManagers["another-controller"])
}

func TestSlidingWindow_WindowPruning(t *testing.T) {
	t.Parallel()

	var now time.Time

	sw := NewSlidingWindow(SlidingWindowConfig{
		Window: 1 * time.Minute,
		Clock:  func() time.Time { return now },
	})

	ref := testRef("test-cm")

	now = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	sw.RecordReconcile(ref, types.ReconcileOutcome{
		Action:           "Updated",
		ConflictDetected: true,
	})

	now = now.Add(30 * time.Second)

	sw.RecordReconcile(ref, types.ReconcileOutcome{
		Action: "Idle",
	})

	snap := sw.Snapshot(ref)
	assert.Equal(t, 2, snap.ReconcileCount)

	now = now.Add(31 * time.Second)
	snap = sw.Snapshot(ref)
	assert.Equal(t, 1, snap.ReconcileCount)
	assert.Equal(t, 0, snap.ConflictCount)

	now = now.Add(30 * time.Second)
	snap = sw.Snapshot(ref)
	assert.Equal(t, 0, snap.ReconcileCount)
}

func TestSlidingWindow_PruneRemovesEmptyTrackers(t *testing.T) {
	t.Parallel()

	var now time.Time

	sw := NewSlidingWindow(SlidingWindowConfig{
		Window: 1 * time.Minute,
		Clock:  func() time.Time { return now },
	})

	ref := testRef("test-cm")

	now = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Idle"})

	sw.mu.Lock()
	assert.Len(t, sw.objects, 1)
	sw.mu.Unlock()

	now = now.Add(2 * time.Minute)

	sw.RecordTeardown(ref, true)

	sw.mu.Lock()
	_, hasRef := sw.objects[ref]
	trackerCount := len(sw.objects)
	sw.mu.Unlock()
	assert.True(t, hasRef)
	assert.Equal(t, 1, trackerCount)

	now = now.Add(2 * time.Minute)

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Idle"})

	sw.mu.Lock()
	tracker := sw.objects[ref]
	sw.mu.Unlock()
	assert.Equal(t, 1, tracker.count)
	assert.Equal(t, types.ActionIdle, tracker.buf[tracker.head].outcome.Action)
}

func TestSlidingWindow_Backoff(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})

	ref := testRef("test-cm")

	assert.Equal(t, time.Duration(0), sw.Backoff(ref))

	sw.RecordReconcile(ref, types.ReconcileOutcome{
		Action:           "Recovered",
		ConflictDetected: true,
	})
	assert.Equal(t, 5*time.Second, sw.Backoff(ref))
}

func TestSlidingWindow_CustomBackoffFunc(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
		BackoffFunc: func(snap Snapshot) time.Duration {
			return time.Duration(snap.ReconcileCount) * time.Second
		},
	})

	ref := testRef("test-cm")
	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Idle"})
	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Idle"})
	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Idle"})

	assert.Equal(t, 3*time.Second, sw.Backoff(ref))
}

func TestSlidingWindow_SnapshotTimestamps(t *testing.T) {
	t.Parallel()

	var now time.Time

	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})

	ref := testRef("test-cm")

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(10 * time.Second)
	t3 := t1.Add(20 * time.Second)

	now = t1

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Created"})

	now = t2

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Updated"})

	now = t3

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Idle"})

	snap := sw.Snapshot(ref)
	assert.Equal(t, t1, snap.OldestEvent)
	assert.Equal(t, t3, snap.NewestEvent)
}

func TestSlidingWindow_EmptySnapshot(t *testing.T) {
	t.Parallel()

	sw := NewSlidingWindow(SlidingWindowConfig{})

	snap := sw.Snapshot(testRef("nonexistent"))
	assert.Equal(t, 0, snap.ReconcileCount)
	assert.Equal(t, 0, snap.ConflictCount)
	assert.Empty(t, snap.UniqueManagers)
}

func TestSlidingWindow_MultipleObjects(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})

	ref1 := testRef("cm-1")
	ref2 := testRef("cm-2")

	sw.RecordReconcile(ref1, types.ReconcileOutcome{Action: "Updated", ConflictDetected: true})
	sw.RecordReconcile(ref2, types.ReconcileOutcome{Action: "Idle"})
	sw.RecordReconcile(ref2, types.ReconcileOutcome{Action: "Idle"})

	snap1 := sw.Snapshot(ref1)
	snap2 := sw.Snapshot(ref2)

	assert.Equal(t, 1, snap1.ReconcileCount)
	assert.Equal(t, 1, snap1.ConflictCount)
	assert.Equal(t, 2, snap2.ReconcileCount)
	assert.Equal(t, 0, snap2.ConflictCount)
}

func TestNewSlidingWindow_DefaultMaxEvents(t *testing.T) {
	t.Parallel()

	sw := NewSlidingWindow(SlidingWindowConfig{})
	assert.Equal(t, defaultMaxEventsPerObj, sw.config.MaxEventsPerObject)
}

func TestSlidingWindow_MaxEventsPerObject(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		MaxEventsPerObject: 5,
		Clock:              func() time.Time { return now },
	})

	ref := testRef("capped")

	for i := range 10 {
		now = now.Add(time.Duration(i) * time.Second)

		sw.RecordReconcile(ref, types.ReconcileOutcome{
			Action:           "Recovered",
			ConflictDetected: true,
		})
	}

	sw.mu.Lock()
	tracker := sw.objects[ref]
	eventCount := tracker.count
	oldest := tracker.buf[tracker.head].timestamp
	newest := tracker.buf[(tracker.head+eventCount-1)%len(tracker.buf)].timestamp
	sw.mu.Unlock()

	assert.Equal(t, 5, eventCount)
	assert.True(t, newest.After(oldest))

	snap := sw.Snapshot(ref)
	assert.Equal(t, 5, snap.ReconcileCount)
	assert.Equal(t, 5, snap.ConflictCount)
}

func TestSlidingWindow_MaxEventsDropsOldest(t *testing.T) {
	t.Parallel()

	var now time.Time

	sw := NewSlidingWindow(SlidingWindowConfig{
		MaxEventsPerObject: 3,
		Clock:              func() time.Time { return now },
	})

	ref := testRef("drop-oldest")

	now = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Created"})

	now = now.Add(1 * time.Second)

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Updated"})

	now = now.Add(1 * time.Second)

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Idle"})

	now = now.Add(1 * time.Second)

	sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Recovered"})

	sw.mu.Lock()
	tracker := sw.objects[ref]
	actions := make([]types.Action, 0, tracker.count)

	for i := range tracker.count {
		actions = append(actions, tracker.buf[(tracker.head+i)%len(tracker.buf)].outcome.Action)
	}

	sw.mu.Unlock()

	assert.Equal(t, []types.Action{"Updated", "Idle", "Recovered"}, actions)
}

func TestSlidingWindow_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	const (
		goroutines = 10
		iterations = 100
		maxEvents  = goroutines * iterations
	)

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		MaxEventsPerObject: maxEvents,
		Clock:              func() time.Time { return now },
	})

	ref := testRef("concurrent")

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			for range iterations {
				sw.RecordReconcile(ref, types.ReconcileOutcome{Action: "Idle"})
				sw.Backoff(ref)
				sw.Snapshot(ref)
			}
		}()
	}

	wg.Wait()

	snap := sw.Snapshot(ref)
	assert.Equal(t, maxEvents, snap.ReconcileCount)
}
