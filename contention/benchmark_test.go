package contention

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"pkg.package-operator.run/boxcutter/machinery/types"
)

func benchRef(i int) types.ObjectRef {
	return types.ObjectRef{
		GroupVersionKind: schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		ObjectKey:        client.ObjectKey{Namespace: "ns", Name: fmt.Sprintf("obj-%d", i)},
	}
}

func benchOutcome() types.ReconcileOutcome {
	return types.ReconcileOutcome{
		Action:              types.ActionRecovered,
		ConflictDetected:    true,
		ConflictingManagers: []string{"other-controller"},
	}
}

// --- Record throughput ---

func BenchmarkSlidingWindow_Record(b *testing.B) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})
	ref := benchRef(0)
	outcome := benchOutcome()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sw.RecordReconcile(ref, outcome)
	}
}

func BenchmarkFlowcontrol_Next(b *testing.B) {
	bo := flowcontrol.NewFakeBackOff(
		1*time.Second, 2*time.Minute,
		&fakeClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
	)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bo.Next("obj-0", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	}
}

// --- Query throughput ---

func BenchmarkSlidingWindow_Backoff(b *testing.B) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})
	ref := benchRef(0)

	for range 50 {
		sw.RecordReconcile(ref, benchOutcome())
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sw.Backoff(ref)
	}
}

func BenchmarkFlowcontrol_Get(b *testing.B) {
	bo := flowcontrol.NewFakeBackOff(
		1*time.Second, 2*time.Minute,
		&fakeClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
	)

	for range 50 {
		bo.Next("obj-0", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bo.Get("obj-0")
	}
}

// --- BackoffFunc computation ---

func BenchmarkDefaultBackoff(b *testing.B) {
	snap := Snapshot{
		ConflictCount:  7,
		UniqueManagers: map[string]int{"a": 3, "b": 2, "c": 1},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		DefaultBackoff(snap)
	}
}

func BenchmarkBackoffConfig_BackoffFunc(b *testing.B) {
	fn := DefaultBackoffConfig().BackoffFunc()
	snap := Snapshot{
		ConflictCount:  7,
		UniqueManagers: map[string]int{"a": 3, "b": 2, "c": 1},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		fn(snap)
	}
}

// --- Multi-object scaling ---

func BenchmarkSlidingWindow_Record_ManyObjects(b *testing.B) {
	for _, numObjects := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("objects=%d", numObjects), func(b *testing.B) {
			now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			sw := NewSlidingWindow(SlidingWindowConfig{
				Clock: func() time.Time { return now },
			})

			refs := make([]types.ObjectRef, numObjects)
			for i := range numObjects {
				refs[i] = benchRef(i)
			}

			outcome := benchOutcome()

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				for _, ref := range refs {
					sw.RecordReconcile(ref, outcome)
				}
			}
		})
	}
}

func BenchmarkFlowcontrol_Next_ManyObjects(b *testing.B) {
	for _, numObjects := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("objects=%d", numObjects), func(b *testing.B) {
			fc := &fakeClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
			bo := flowcontrol.NewFakeBackOff(1*time.Second, 2*time.Minute, fc)

			ids := make([]string, numObjects)
			for i := range numObjects {
				ids[i] = fmt.Sprintf("obj-%d", i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				for _, id := range ids {
					bo.Next(id, fc.now)
				}
			}
		})
	}
}

// --- Memory measurement ---

func BenchmarkMemory_Flowcontrol(b *testing.B) {
	for _, numObjects := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("%dobj", numObjects), func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				fc := &fakeClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
				bo := flowcontrol.NewFakeBackOff(1*time.Second, 2*time.Minute, fc)

				for i := range numObjects {
					bo.Next(fmt.Sprintf("obj-%d", i), fc.now)
				}

				runtime.KeepAlive(bo)
			}
		})
	}
}

// --- Record + Backoff combined (simulates reconcile loop) ---

func BenchmarkFlowcontrol_ReconcileLoop(b *testing.B) {
	fc := &fakeClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	bo := flowcontrol.NewFakeBackOff(1*time.Second, 2*time.Minute, fc)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bo.Next("obj-0", fc.now)
		bo.Get("obj-0")
	}
}

// fakeClock implements clock.Clock for flowcontrol benchmarks.
type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time                  { return c.now }
func (c *fakeClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }
func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	return time.After(0)
}

func (c *fakeClock) NewTimer(d time.Duration) clock.Timer {
	panic("not implemented")
}

func (c *fakeClock) NewTicker(d time.Duration) clock.Ticker {
	panic("not implemented")
}

func (c *fakeClock) Sleep(d time.Duration) {
	panic("not implemented")
}

func (c *fakeClock) Tick(d time.Duration) <-chan time.Time {
	panic("not implemented")
}
