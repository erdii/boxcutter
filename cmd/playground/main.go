package main

// import (
// 	"context"
// 	"fmt"
// 	"strconv"

// 	"golang.org/x/sync/errgroup"
// 	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
// 	"k8s.io/apimachinery/pkg/runtime"
// 	"k8s.io/client-go/discovery"
// 	"k8s.io/client-go/kubernetes/scheme"
// 	ctrl "sigs.k8s.io/controller-runtime"
// 	"sigs.k8s.io/controller-runtime/pkg/client"
// 	"sigs.k8s.io/controller-runtime/pkg/log/zap"

// 	"pkg.package-operator.run/boxcutter/machinery"
// 	"pkg.package-operator.run/boxcutter/machinery/types"
// )

// const (
// 	fieldOwner   = "hurpendurpen"
// 	systemPrefix = "hurpendurpen.inator"
// )

// var (
// 	Client          client.Client
// 	DiscoveryClient discovery.DiscoveryInterface
// 	Scheme          *runtime.Scheme
// )

// func main() {
// 	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

// 	Scheme := runtime.NewScheme()
// 	if err := scheme.AddToScheme(Scheme); err != nil {
// 		panic(err)
// 	}

// 	config := ctrl.GetConfigOrDie()

// 	var err error

// 	Client, err = client.New(config, client.Options{})
// 	if err != nil {
// 		panic(err)
// 	}

// 	DiscoveryClient, err = discovery.NewDiscoveryClientForConfig(config)
// 	if err != nil {
// 		panic(err)
// 	}

// 	if err := start(); err != nil {
// 		panic(err)
// 	}
// }

// type MockValidator struct{}

// func (MockValidator) Validate(
// 	_ context.Context,
// 	_ types.Phase,
// 	_ ...types.PhaseReconcileOption,
// ) error {
// 	return nil
// }

// func start() error {
// 	comp := machinery.NewComparator(DiscoveryClient, Scheme, fieldOwner)
// 	oe := machinery.NewObjectEngine(
// 		Scheme, Client, comp, fieldOwner, systemPrefix, "", Client,
// 	)

// 	pe := machinery.NewPhaseEngine(oe, MockValidator{})

// 	ctx := context.Background()

// 	for x := range 1 {
// 		eg, ectx := errgroup.WithContext(ctx)
// 		for i := range 1000 {
// 			eg.Go(func() error {
// 				_, err := pe.Reconcile(ectx, 1, types.NewPhase("a", []client.Object{
// 					&unstructured.Unstructured{
// 						Object: map[string]interface{}{
// 							"apiVersion": "v1",
// 							"kind":       "ConfigMap",
// 							"metadata": map[string]any{
// 								"namespace": "default",
// 								"name":      fmt.Sprintf("test-x-%d-%d", x, i),
// 							},
// 							"data": map[string]any{
// 								"blurb": strconv.Itoa(i),
// 							},
// 						},
// 					},
// 					&unstructured.Unstructured{
// 						Object: map[string]interface{}{
// 							"apiVersion": "v1",
// 							"kind":       "ConfigMap",
// 							"metadata": map[string]any{
// 								"namespace": "default",
// 								"name":      fmt.Sprintf("test-x-%d-%d", x, i),
// 							},
// 							"data": map[string]any{
// 								"blurb": strconv.Itoa(i),
// 							},
// 						},
// 					},
// 				}), types.WithAggregatePhaseReconcileErrors())

// 				return err
// 			})
// 		}

// 		err := eg.Wait()
// 		if err != nil {
// 			fmt.Println(err)

// 			return err
// 		}
// 	}

// 	return nil
// }
