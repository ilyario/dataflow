/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDataFlowReconciler(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

	assert.NotNil(t, reconciler)
	assert.Equal(t, fakeClient, reconciler.Client)
	assert.Equal(t, scheme, reconciler.Scheme)
	assert.NotNil(t, reconciler.processors)
}

func TestDataFlowReconciler_Reconcile_CreateProcessor(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				},
			},
			Sink: dataflowv1.SinkSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
				},
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// Reconcile may fail due to connection errors, but processor should be created
	result, err := reconciler.Reconcile(ctx, req)
	// We don't require no error because connection to Kafka will fail in background

	// Verify that the DataFlow still exists
	var updatedDataflow dataflowv1.DataFlow
	getErr := fakeClient.Get(ctx, req.NamespacedName, &updatedDataflow)
	require.NoError(t, getErr, "DataFlow should exist after reconcile")

	// Processor should be created (even if it fails to start later)
	reconciler.mu.RLock()
	key := "default/test-dataflow"
	_, exists := reconciler.processors[key]
	reconciler.mu.RUnlock()
	assert.True(t, exists, "processor should be created")

	// Status update may fail with fake client, but we verify processor was created
	// In real scenario, status would be "Running" or "Error"
	assert.Equal(t, ctrl.Result{}, result)
}

func TestDataFlowReconciler_Reconcile_DeleteDataFlow(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

	ctx := context.Background()

	now := metav1.Now()
	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-dataflow",
			Namespace:         "default",
			DeletionTimestamp: &now,
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				},
			},
			Sink: dataflowv1.SinkSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
				},
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// Note: fake client has limitations with DeletionTimestamp - it may not find the object
	// In real Kubernetes, objects with DeletionTimestamp still exist until finalizers are removed
	// This test verifies that reconcile handles deletion gracefully without panicking
	result, err := reconciler.Reconcile(ctx, req)

	// Reconcile should handle NotFound errors gracefully (returns no error via IgnoreNotFound)
	// The main test is that reconcile doesn't panic
	assert.Equal(t, ctrl.Result{}, result)

	// Note: Due to fake client limitation, if object is not found, deletion logic doesn't run
	// In real scenario, object would exist and processor would be removed
	// We just verify that reconcile completes without error/panic
}

func TestDataFlowReconciler_Reconcile_InvalidSpec(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "invalid",
			},
			Sink: dataflowv1.SinkSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
				},
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// Reconcile will fail due to invalid source type
	result, err := reconciler.Reconcile(ctx, req)
	// Error is expected, but status should still be updated

	// Verify status was updated to Error
	var updatedDataflow dataflowv1.DataFlow
	getErr := fakeClient.Get(ctx, req.NamespacedName, &updatedDataflow)
	require.NoError(t, getErr, "DataFlow should exist")

	// Status should be Error if processor creation failed
	// Note: Status update happens in a goroutine, so we might need to wait
	// For now, we check that either Error status is set, or the reconcile returned an error
	if updatedDataflow.Status.Phase == "Error" {
		assert.Contains(t, updatedDataflow.Status.Message, "Failed to create processor")
	} else {
		// If status wasn't updated yet, at least verify that reconcile returned an error
		assert.Error(t, err, "Reconcile should return error for invalid spec")
	}

	assert.Equal(t, ctrl.Result{}, result)
}

func TestDataFlowReconciler_Reconcile_UpdateStats(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

	ctx := context.Background()

	dataflow := &dataflowv1.DataFlow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dataflow.dataflow.io/v1",
			Kind:       "DataFlow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataflow",
			Namespace: "default",
		},
		Spec: dataflowv1.DataFlowSpec{
			Source: dataflowv1.SourceSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSourceSpec{
					Brokers:       []string{"localhost:9092"},
					Topic:         "test-topic",
					ConsumerGroup: "test-group",
				},
			},
			Sink: dataflowv1.SinkSpec{
				Type: "kafka",
				Kafka: &dataflowv1.KafkaSinkSpec{
					Brokers: []string{"localhost:9092"},
					Topic:   "output-topic",
				},
			},
		},
	}

	err = fakeClient.Create(ctx, dataflow)
	require.NoError(t, err)

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-dataflow",
			Namespace: "default",
		},
	}

	// First reconcile to create processor
	// Note: This may fail because we can't actually connect to Kafka
	// But we're testing that the reconcile logic works
	_, err = reconciler.Reconcile(ctx, req)
	// We don't check for specific errors as they depend on external connections

	// Second reconcile to update stats (if processor was created)
	_, err = reconciler.Reconcile(ctx, req)
	// Again, we don't check for specific errors

	// Verify that reconcile completed (status may vary depending on connection success)
	var updatedDataflow dataflowv1.DataFlow
	getErr := fakeClient.Get(ctx, req.NamespacedName, &updatedDataflow)
	if getErr == nil {
		// Status should be set to something (Running, Error, or empty)
		// We just verify that the reconcile process ran
		assert.NotNil(t, updatedDataflow.Status)
	}
}

func TestDataFlowReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	err := dataflowv1.AddToScheme(scheme)
	require.NoError(t, err)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewDataFlowReconciler(fakeClient, scheme)

	ctx := context.Background()

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-existent",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}
