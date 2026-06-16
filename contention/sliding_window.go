package contention

import (
	"slices"
	"sync"
	"time"

	"pkg.package-operator.run/boxcutter/machinery/types"
)

const (
	defaultWindow          = 5 * time.Minute
	defaultMaxEventsPerObj = 512
)

// SlidingWindowConfig configures a SlidingWindow.
type SlidingWindowConfig struct {
	// Window is the rolling time window for tracking events.
	// Events older than this are discarded. Default: 5 minutes.
	Window time.Duration
	// MaxEventsPerObject caps the number of events retained per object.
	// When full, the oldest event is dropped to make room.
	// Default: 512.
	MaxEventsPerObject int
	// BackoffFunc computes the backoff from a Snapshot.
	// If nil, DefaultBackoff is used.
	BackoffFunc BackoffFunc
	// Clock overrides time.Now for testing.
	Clock func() time.Time
}

func (c *SlidingWindowConfig) setDefaults() {
	if c.Window <= 0 {
		c.Window = defaultWindow
	}

	if c.MaxEventsPerObject <= 0 {
		c.MaxEventsPerObject = defaultMaxEventsPerObj
	}

	if c.BackoffFunc == nil {
		c.BackoffFunc = DefaultBackoff
	}

	if c.Clock == nil {
		c.Clock = time.Now
	}
}

// SlidingWindow is a ContentionObserver backed by a sliding time window.
type SlidingWindow struct {
	mu      sync.Mutex
	config  SlidingWindowConfig
	objects map[types.ObjectRef]*objectTracker
}

// NewSlidingWindow creates a new SlidingWindow with the given configuration.
func NewSlidingWindow(config SlidingWindowConfig) *SlidingWindow {
	config.setDefaults()

	return &SlidingWindow{
		config:  config,
		objects: make(map[types.ObjectRef]*objectTracker),
	}
}

// RecordReconcile records a reconcile outcome for the given object.
func (sw *SlidingWindow) RecordReconcile(ref types.ObjectRef, outcome types.ReconcileOutcome) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := sw.config.Clock()
	tracker := sw.getOrCreateTracker(ref)
	tracker.record(event{
		timestamp: now,
		kind:      eventKindReconcile,
		outcome:   cloneOutcome(outcome),
	})
	sw.pruneTrackerAt(ref, tracker, now)
}

// RecordTeardown records a teardown attempt for the given object.
func (sw *SlidingWindow) RecordTeardown(ref types.ObjectRef, gone bool) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := sw.config.Clock()
	tracker := sw.getOrCreateTracker(ref)
	tracker.record(event{
		timestamp: now,
		kind:      eventKindTeardown,
		gone:      gone,
	})
	sw.pruneTrackerAt(ref, tracker, now)
}

// Backoff returns a recommended backoff duration for the given object.
func (sw *SlidingWindow) Backoff(ref types.ObjectRef) time.Duration {
	snap := sw.Snapshot(ref)

	return sw.config.BackoffFunc(snap)
}

// Snapshot returns an aggregated view of contention data for the given object.
func (sw *SlidingWindow) Snapshot(ref types.ObjectRef) Snapshot {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	tracker, ok := sw.objects[ref]
	if !ok {
		return Snapshot{}
	}

	cutoff := sw.config.Clock().Add(-sw.config.Window)
	tracker.prune(cutoff)

	if tracker.count == 0 {
		delete(sw.objects, ref)

		return Snapshot{}
	}

	return tracker.snapshot()
}

func (sw *SlidingWindow) getOrCreateTracker(ref types.ObjectRef) *objectTracker {
	tracker, ok := sw.objects[ref]
	if !ok {
		tracker = &objectTracker{maxEvents: sw.config.MaxEventsPerObject}
		sw.objects[ref] = tracker
	}

	return tracker
}

func (sw *SlidingWindow) pruneTrackerAt(ref types.ObjectRef, tracker *objectTracker, now time.Time) {
	cutoff := now.Add(-sw.config.Window)
	tracker.prune(cutoff)

	if tracker.count == 0 {
		delete(sw.objects, ref)
	}
}

// Snapshot is a point-in-time aggregation of contention data for one object.
type Snapshot struct {
	ReconcileCount int
	TeardownCount  int
	ConflictCount  int
	RecoveryCount  int
	CollisionCount int
	UniqueManagers map[string]int
	OldestEvent    time.Time
	NewestEvent    time.Time
}

type eventKind int

const (
	eventKindReconcile eventKind = iota
	eventKindTeardown
)

type event struct {
	timestamp time.Time
	kind      eventKind
	outcome   types.ReconcileOutcome
	gone      bool
}

type objectTracker struct {
	buf       []event
	head      int
	count     int
	maxEvents int
}

func (t *objectTracker) record(e event) {
	if t.maxEvents > 0 && t.count >= t.maxEvents {
		t.buf[t.head] = e
		t.head = (t.head + 1) % t.maxEvents

		return
	}

	t.buf = append(t.buf, e)
	t.count++
}

func (t *objectTracker) prune(cutoff time.Time) {
	if t.head == 0 {
		n := 0

		for _, e := range t.buf[:t.count] {
			if !e.timestamp.Before(cutoff) {
				t.buf[n] = e
				n++
			}
		}

		clear(t.buf[n:t.count])
		t.buf = t.buf[:n]
		t.count = n

		return
	}

	n := 0
	tmp := make([]event, 0, t.count)

	for i := range t.count {
		e := t.buf[(t.head+i)%len(t.buf)]

		if !e.timestamp.Before(cutoff) {
			tmp = append(tmp, e)
			n++
		}
	}

	copy(t.buf, tmp)
	clear(t.buf[n:])
	t.buf = t.buf[:n]
	t.head = 0
	t.count = n
}

func (t *objectTracker) snapshot() Snapshot {
	snap := Snapshot{
		UniqueManagers: make(map[string]int),
	}

	for i := range t.count {
		e := t.buf[(t.head+i)%len(t.buf)]

		if snap.OldestEvent.IsZero() || e.timestamp.Before(snap.OldestEvent) {
			snap.OldestEvent = e.timestamp
		}

		if e.timestamp.After(snap.NewestEvent) {
			snap.NewestEvent = e.timestamp
		}

		switch e.kind {
		case eventKindReconcile:
			snap.ReconcileCount++
			if e.outcome.ConflictDetected {
				snap.ConflictCount++
			}

			if e.outcome.Action == types.ActionRecovered {
				snap.RecoveryCount++
			}

			if e.outcome.Action == types.ActionCollision {
				snap.CollisionCount++
			}

			for _, mgr := range e.outcome.ConflictingManagers {
				snap.UniqueManagers[mgr]++
			}
		case eventKindTeardown:
			snap.TeardownCount++
		}
	}

	return snap
}

func cloneOutcome(o types.ReconcileOutcome) types.ReconcileOutcome {
	out := types.ReconcileOutcome{
		Action:              o.Action,
		ConflictDetected:    o.ConflictDetected,
		ConflictingManagers: slices.Clone(o.ConflictingManagers),
	}

	if len(o.FieldConflicts) > 0 {
		out.FieldConflicts = make([]types.FieldConflict, len(o.FieldConflicts))
		for i, fc := range o.FieldConflicts {
			out.FieldConflicts[i] = types.FieldConflict{
				Manager: fc.Manager,
				Fields:  slices.Clone(fc.Fields),
			}
		}
	}

	return out
}
