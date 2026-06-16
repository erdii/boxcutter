package machinery

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/typed"

	"pkg.package-operator.run/boxcutter/internal/testutil"
	"pkg.package-operator.run/boxcutter/machinery/types"
)

type contentionObserverMock struct {
	mock.Mock
}

func (m *contentionObserverMock) RecordReconcile(ref types.ObjectRef, outcome types.ReconcileOutcome) {
	m.Called(ref, outcome)
}

func (m *contentionObserverMock) RecordTeardown(ref types.ObjectRef, gone bool) {
	m.Called(ref, gone)
}

func TestRecordReconcile_NilObserver(t *testing.T) {
	t.Parallel()

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"})
	obj.SetName("test")
	obj.SetNamespace("default")

	result := ObjectResultIdle{
		normalResult: normalResult{
			action: ActionIdle,
			obj:    obj,
		},
	}

	recordReconcile(nil, obj, result, CompareResult{}, false)
}

func TestRecordTeardown_NilObserver(t *testing.T) {
	t.Parallel()

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"})
	obj.SetName("test")
	obj.SetNamespace("default")

	recordTeardown(nil, obj, true)
}

func TestObjectEngine_Reconcile_RecordsToContentionObserver(t *testing.T) {
	t.Parallel()

	t.Run("Created", func(t *testing.T) {
		t.Parallel()

		cache := &cacheMock{}
		writer := testutil.NewClient()
		comp := &comparatorMock{}
		observer := &contentionObserverMock{}

		oe := NewObjectEngine(scheme.Scheme, cache, writer, comp,
			testFieldOwner, testSystemPrefix, "", nil)

		desired := buildObj("test", "default")(&withoutOwnerMode)

		cache.
			On("Get", mock.Anything, client.ObjectKey{Name: "test", Namespace: "default"}, mock.Anything, mock.Anything).
			Return(apierrors.NewNotFound(schema.GroupResource{}, ""))
		writer.
			On("Create", mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		observer.On("RecordReconcile", mock.Anything, mock.Anything).Return()

		res, err := oe.Reconcile(t.Context(), 1, desired,
			types.WithContentionObserver(observer))

		require.NoError(t, err)
		assert.Equal(t, ActionCreated, res.Action())

		observer.AssertCalled(t, "RecordReconcile",
			mock.MatchedBy(func(ref types.ObjectRef) bool {
				return ref.Name == "test" && ref.Namespace == "default"
			}),
			types.ReconcileOutcome{
				Action:           "Created",
				ConflictDetected: false,
			},
		)
	})

	t.Run("Idle", func(t *testing.T) {
		t.Parallel()

		cache := &cacheMock{}
		writer := testutil.NewClient()
		comp := &comparatorMock{}
		observer := &contentionObserverMock{}

		oe := NewObjectEngine(scheme.Scheme, cache, writer, comp,
			testFieldOwner, testSystemPrefix, "", nil)

		actual := buildObj("test", "default", withRevision("1"), withBoxcutterManagedLabel)(&withoutOwnerMode)
		desired := buildObj("test", "default")(&withoutOwnerMode)

		cache.
			On("Get", mock.Anything, client.ObjectKeyFromObject(actual), mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				obj := args.Get(2).(*unstructured.Unstructured)
				*obj = *actual
			}).
			Return(nil)
		comp.
			On("Compare", mock.Anything, mock.Anything, mock.Anything).
			Return(CompareResult{}, nil)

		observer.On("RecordReconcile", mock.Anything, mock.Anything).Return()

		res, err := oe.Reconcile(t.Context(), 1, desired,
			types.WithContentionObserver(observer))

		require.NoError(t, err)
		assert.Equal(t, ActionIdle, res.Action())

		observer.AssertCalled(t, "RecordReconcile",
			mock.MatchedBy(func(ref types.ObjectRef) bool {
				return ref.Name == "test"
			}),
			types.ReconcileOutcome{
				Action:           "Idle",
				ConflictDetected: false,
			},
		)
	})

	t.Run("Recovered with conflicts", func(t *testing.T) {
		t.Parallel()

		cache := &cacheMock{}
		writer := testutil.NewClient()
		comp := &comparatorMock{}
		observer := &contentionObserverMock{}

		oe := NewObjectEngine(scheme.Scheme, cache, writer, comp,
			testFieldOwner, testSystemPrefix, "", nil)

		actual := buildObj("test", "default", withRevision("1"), withBoxcutterManagedLabel)(&withoutOwnerMode)
		desired := buildObj("test", "default")(&withoutOwnerMode)

		fs := &fieldpath.Set{}
		fs.Insert(fieldpath.MakePathOrDie("spec", "data"))
		conflictCompare := CompareResult{
			ConflictingMangers: []CompareResultManagedFields{
				{Manager: "other-controller", Fields: fs},
			},
			Comparison: &typed.Comparison{
				Modified: &fieldpath.Set{},
				Removed:  &fieldpath.Set{},
			},
		}

		cache.
			On("Get", mock.Anything, client.ObjectKeyFromObject(actual), mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				obj := args.Get(2).(*unstructured.Unstructured)
				*obj = *actual
			}).
			Return(nil)
		comp.
			On("Compare", mock.Anything, mock.Anything, mock.Anything).
			Return(conflictCompare, nil)
		writer.
			On("Apply", mock.Anything, mock.Anything, mock.Anything).
			Return(nil)
		writer.
			On("Patch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		observer.On("RecordReconcile", mock.Anything, mock.Anything).Return()

		res, err := oe.Reconcile(t.Context(), 1, desired,
			types.WithContentionObserver(observer))

		require.NoError(t, err)
		assert.Equal(t, ActionRecovered, res.Action())

		observer.AssertCalled(t, "RecordReconcile",
			mock.MatchedBy(func(ref types.ObjectRef) bool {
				return ref.Name == "test"
			}),
			types.ReconcileOutcome{
				Action:              "Recovered",
				ConflictDetected:    true,
				ConflictingManagers: []string{"other-controller"},
			},
		)
	})

	t.Run("Collision", func(t *testing.T) {
		t.Parallel()

		cache := &cacheMock{}
		writer := testutil.NewClient()
		comp := &comparatorMock{}
		observer := &contentionObserverMock{}

		oe := NewObjectEngine(scheme.Scheme, cache, writer, comp,
			testFieldOwner, testSystemPrefix, "", nil)

		actual := buildObj("test", "default")(&withoutOwnerMode)
		desired := buildObj("test", "default")(&withoutOwnerMode)

		cache.
			On("Get", mock.Anything, client.ObjectKeyFromObject(actual), mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				obj := args.Get(2).(*unstructured.Unstructured)
				*obj = *actual
			}).
			Return(nil)
		comp.
			On("Compare", mock.Anything, mock.Anything, mock.Anything).
			Return(CompareResult{}, nil)

		observer.On("RecordReconcile", mock.Anything, mock.Anything).Return()

		res, err := oe.Reconcile(t.Context(), 1, desired,
			types.WithContentionObserver(observer),
			types.WithCollisionProtection(types.CollisionProtectionPrevent))

		require.NoError(t, err)
		assert.Equal(t, ActionCollision, res.Action())

		observer.AssertCalled(t, "RecordReconcile",
			mock.MatchedBy(func(ref types.ObjectRef) bool {
				return ref.Name == "test"
			}),
			types.ReconcileOutcome{
				Action:           "Collision",
				ConflictDetected: false,
			},
		)
	})
}

func TestObjectEngine_Teardown_RecordsToContentionObserver(t *testing.T) {
	t.Parallel()

	t.Run("object not found skips recording", func(t *testing.T) {
		t.Parallel()

		cache := &cacheMock{}
		writer := testutil.NewClient()
		comp := &comparatorMock{}
		observer := &contentionObserverMock{}

		oe := NewObjectEngine(scheme.Scheme, cache, writer, comp,
			testFieldOwner, testSystemPrefix, "", nil)

		desired := buildObj("test", "default")(&withoutOwnerMode)

		cache.
			On("Get", mock.Anything, client.ObjectKey{Name: "test", Namespace: "default"}, mock.Anything, mock.Anything).
			Return(apierrors.NewNotFound(schema.GroupResource{}, ""))

		gone, err := oe.Teardown(t.Context(), 1, desired,
			types.WithContentionObserver(observer))

		require.NoError(t, err)
		assert.True(t, gone)

		observer.AssertNotCalled(t, "RecordTeardown", mock.Anything, mock.Anything)
	})

	t.Run("object deleted", func(t *testing.T) {
		t.Parallel()

		cache := &cacheMock{}
		writer := testutil.NewClient()
		comp := &comparatorMock{}
		observer := &contentionObserverMock{}

		oe := NewObjectEngine(scheme.Scheme, cache, writer, comp,
			testFieldOwner, testSystemPrefix, "", nil)

		actual := buildObj("test", "default", withRevision("1"), withResourceVersion("42"))(&withoutOwnerMode)
		desired := buildObj("test", "default")(&withoutOwnerMode)

		cache.
			On("Get", mock.Anything, client.ObjectKeyFromObject(actual), mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				obj := args.Get(2).(*unstructured.Unstructured)
				*obj = *actual
			}).
			Return(nil)
		writer.
			On("Delete", mock.Anything, mock.Anything, mock.Anything).
			Return(nil)

		observer.On("RecordTeardown", mock.Anything, mock.Anything).Return()

		gone, err := oe.Teardown(t.Context(), 1, desired,
			types.WithContentionObserver(observer))

		require.NoError(t, err)
		assert.False(t, gone)

		observer.AssertCalled(t, "RecordTeardown",
			mock.MatchedBy(func(ref types.ObjectRef) bool {
				return ref.Name == "test"
			}),
			false,
		)
	})
}

func TestObjectEngine_Reconcile_RecordsOnError(t *testing.T) {
	t.Parallel()

	cache := &cacheMock{}
	writer := testutil.NewClient()
	comp := &comparatorMock{}
	observer := &contentionObserverMock{}

	oe := NewObjectEngine(scheme.Scheme, cache, writer, comp,
		testFieldOwner, testSystemPrefix, "", nil)

	actual := buildObj("test", "default", withRevision("1"), withBoxcutterManagedLabel)(&withoutOwnerMode)
	desired := buildObj("test", "default")(&withoutOwnerMode)

	fs := &fieldpath.Set{}
	fs.Insert(fieldpath.MakePathOrDie("spec", "data"))
	conflictCompare := CompareResult{
		ConflictingMangers: []CompareResultManagedFields{
			{Manager: "other-controller", Fields: fs},
		},
		Comparison: &typed.Comparison{
			Modified: &fieldpath.Set{},
			Removed:  &fieldpath.Set{},
		},
	}

	cache.
		On("Get", mock.Anything, client.ObjectKeyFromObject(actual), mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			obj := args.Get(2).(*unstructured.Unstructured)
			*obj = *actual
		}).
		Return(nil)
	comp.
		On("Compare", mock.Anything, mock.Anything, mock.Anything).
		Return(conflictCompare, nil)
	writer.
		On("Apply", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("apply failed"))
	writer.
		On("Patch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("patch failed"))

	observer.On("RecordReconcile", mock.Anything, mock.Anything).Return()

	_, err := oe.Reconcile(t.Context(), 1, desired,
		types.WithContentionObserver(observer))

	require.Error(t, err)

	observer.AssertCalled(t, "RecordReconcile",
		mock.MatchedBy(func(ref types.ObjectRef) bool {
			return ref.Name == "test"
		}),
		mock.MatchedBy(func(outcome types.ReconcileOutcome) bool {
			return outcome.Action == "" &&
				outcome.ConflictDetected &&
				len(outcome.ConflictingManagers) == 1 &&
				outcome.ConflictingManagers[0] == "other-controller"
		}),
	)
}

func TestObjectEngine_Teardown_SkipsRecordingOnOrphan(t *testing.T) {
	t.Parallel()

	cache := &cacheMock{}
	writer := testutil.NewClient()
	comp := &comparatorMock{}
	observer := &contentionObserverMock{}

	oe := NewObjectEngine(scheme.Scheme, cache, writer, comp,
		testFieldOwner, testSystemPrefix, "", nil)

	desired := buildObj("test", "default")(&withoutOwnerMode)

	writer.
		On("Patch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	gone, err := oe.Teardown(t.Context(), 1, desired,
		types.WithContentionObserver(observer),
		types.WithOrphan())

	require.NoError(t, err)
	assert.True(t, gone)

	observer.AssertNotCalled(t, "RecordTeardown", mock.Anything, mock.Anything)
}

func TestRecordReconcile_ExtractsManagers(t *testing.T) {
	t.Parallel()

	observer := &contentionObserverMock{}
	observer.On("RecordReconcile", mock.Anything, mock.Anything).Return()

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"})
	obj.SetName("test")
	obj.SetNamespace("default")

	fs := &fieldpath.Set{}
	compareRes := CompareResult{
		ConflictingMangers: []CompareResultManagedFields{
			{Manager: "mgr-a", Fields: fs},
			{Manager: "mgr-b", Fields: fs},
		},
	}
	result := ObjectResultRecovered{
		normalResult: normalResult{
			action:        ActionRecovered,
			obj:           obj,
			compareResult: compareRes,
		},
	}

	recordReconcile(observer, obj, result, compareRes, false)

	observer.AssertCalled(t, "RecordReconcile",
		mock.Anything,
		mock.MatchedBy(func(outcome types.ReconcileOutcome) bool {
			return outcome.ConflictDetected &&
				len(outcome.ConflictingManagers) == 2 &&
				outcome.ConflictingManagers[0] == "mgr-a" &&
				outcome.ConflictingManagers[1] == "mgr-b"
		}),
	)
}

var _ types.ContentionObserver = (*contentionObserverMock)(nil)
