package types

// ContentionObserver tracks reconcile and teardown activity per object.
type ContentionObserver interface {
	// RecordReconcile records a reconcile outcome for the given object.
	RecordReconcile(ref ObjectRef, outcome ReconcileOutcome)
	// RecordTeardown records a teardown attempt for the given object.
	RecordTeardown(ref ObjectRef, gone bool)
}

// ReconcileOutcome summarizes the relevant parts of a reconcile result
// for contention tracking.
type ReconcileOutcome struct {
	// Action taken. Uses the Action* constants (e.g. ActionRecovered).
	// Empty when the reconcile attempt failed with an error.
	Action Action
	// ConflictDetected is true when field ownership conflicts were found.
	ConflictDetected bool
	// ConflictingManagers lists the field manager names that conflicted.
	ConflictingManagers []string
	// FieldConflicts provides per-manager conflicting field paths.
	// Only populated when WithDetailedConflicts is set.
	FieldConflicts []FieldConflict
}

// FieldConflict associates a field manager with the field paths it conflicts on.
type FieldConflict struct {
	// Manager is the name of the conflicting field manager.
	Manager string
	// Fields lists the conflicting field paths as strings.
	Fields []string
}
