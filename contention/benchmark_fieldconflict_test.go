package contention

import (
	"runtime"
	"testing"
	"time"

	"pkg.package-operator.run/boxcutter/machinery/types"
)

// realisticFieldConflicts returns field conflicts typical of a real
// Deployment conflict: 3 managers fighting over container spec,
// resource limits, and env vars. ~15 field paths total.
func realisticFieldConflicts() []types.FieldConflict {
	return []types.FieldConflict{
		{
			Manager: "kubectl-client-side-apply",
			Fields: []string{
				".spec.template.spec.containers[name=app].image",
				".spec.template.spec.containers[name=app].resources.limits.cpu",
				".spec.template.spec.containers[name=app].resources.limits.memory",
				".spec.template.spec.containers[name=app].resources.requests.cpu",
				".spec.template.spec.containers[name=app].resources.requests.memory",
			},
		},
		{
			Manager: "vpa-recommender",
			Fields: []string{
				".spec.template.spec.containers[name=app].resources.limits.cpu",
				".spec.template.spec.containers[name=app].resources.limits.memory",
				".spec.template.spec.containers[name=app].resources.requests.cpu",
				".spec.template.spec.containers[name=app].resources.requests.memory",
			},
		},
		{
			Manager: "istio-sidecar-injector",
			Fields: []string{
				".spec.template.spec.containers[name=istio-proxy].image",
				".spec.template.spec.containers[name=istio-proxy].ports",
				".spec.template.spec.containers[name=istio-proxy].env",
				".spec.template.spec.initContainers[name=istio-init].image",
				".spec.template.spec.initContainers[name=istio-init].args",
				".spec.template.metadata.annotations.sidecar.istio.io/status",
			},
		},
	}
}

func detailedBenchOutcome() types.ReconcileOutcome {
	conflicts := realisticFieldConflicts()
	managers := make([]string, 0, len(conflicts))

	for _, c := range conflicts {
		managers = append(managers, c.Manager)
	}

	return types.ReconcileOutcome{
		Action:              types.ActionRecovered,
		ConflictDetected:    true,
		ConflictingManagers: managers,
		FieldConflicts:      conflicts,
	}
}

// --- Record throughput comparison ---

func BenchmarkRecord_Compact(b *testing.B) {
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

func BenchmarkRecord_Detailed(b *testing.B) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})
	ref := benchRef(0)
	outcome := detailedBenchOutcome()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sw.RecordReconcile(ref, outcome)
	}
}

// --- Memory comparison at scale ---

func BenchmarkMemory_Compact(b *testing.B) {
	for _, scenario := range memoryScenarios() {
		b.Run(scenario.name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
				sw := NewSlidingWindow(SlidingWindowConfig{
					Clock: func() time.Time { return now },
				})

				for i := range scenario.numObjects {
					ref := benchRef(i)
					for range scenario.eventsEach {
						sw.RecordReconcile(ref, benchOutcome())
					}
				}

				runtime.KeepAlive(sw)
			}
		})
	}
}

func BenchmarkMemory_Detailed(b *testing.B) {
	for _, scenario := range memoryScenarios() {
		b.Run(scenario.name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
				sw := NewSlidingWindow(SlidingWindowConfig{
					Clock: func() time.Time { return now },
				})

				for i := range scenario.numObjects {
					ref := benchRef(i)
					for range scenario.eventsEach {
						sw.RecordReconcile(ref, detailedBenchOutcome())
					}
				}

				runtime.KeepAlive(sw)
			}
		})
	}
}

type memoryScenario struct {
	name       string
	numObjects int
	eventsEach int
}

func memoryScenarios() []memoryScenario {
	return []memoryScenario{
		{"1000obj_10ev", 1000, 10},
		{"1000obj_50ev", 1000, 50},
		{"10000obj_10ev", 10000, 10},
	}
}

// --- Reconcile loop comparison ---

func BenchmarkReconcileLoop_Compact(b *testing.B) {
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
		sw.Backoff(ref)
	}
}

func BenchmarkReconcileLoop_Detailed(b *testing.B) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sw := NewSlidingWindow(SlidingWindowConfig{
		Clock: func() time.Time { return now },
	})
	ref := benchRef(0)
	outcome := detailedBenchOutcome()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sw.RecordReconcile(ref, outcome)
		sw.Backoff(ref)
	}
}
