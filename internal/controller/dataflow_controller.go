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
	"fmt"
	"reflect"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/processor"
)

// DataFlowReconciler reconciles a DataFlow object
type DataFlowReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	processors     map[string]*processorContext
	mu             sync.RWMutex
	secretResolver *SecretResolver
}

type processorContext struct {
	processor *processor.Processor
	cancel    context.CancelFunc
	spec      dataflowv1.DataFlowSpec
}

func NewDataFlowReconciler(client client.Client, scheme *runtime.Scheme) *DataFlowReconciler {
	return &DataFlowReconciler{
		Client:         client,
		Scheme:         scheme,
		processors:     make(map[string]*processorContext),
		secretResolver: NewSecretResolver(client),
	}
}

// updateStatusWithRetry обновляет статус DataFlow с retry логикой для обработки конфликтов оптимистичной блокировки
func (r *DataFlowReconciler) updateStatusWithRetry(ctx context.Context, req ctrl.Request, updateFn func(*dataflowv1.DataFlow)) error {
	log := log.FromContext(ctx)
	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		var df dataflowv1.DataFlow
		if err := r.Get(ctx, req.NamespacedName, &df); err != nil {
			if attempt < maxRetries-1 {
				log.Error(err, "unable to fetch DataFlow for status update, retrying", "attempt", attempt+1)
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("failed to fetch DataFlow after %d attempts: %w", maxRetries, err)
		}

		// Применяем функцию обновления статуса
		updateFn(&df)

		if err := r.Status().Update(ctx, &df); err != nil {
			if apierrors.IsConflict(err) {
				if attempt < maxRetries-1 {
					log.Info("status update conflict, retrying", "attempt", attempt+1, "maxRetries", maxRetries)
					time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
					continue
				}
				return fmt.Errorf("failed to update status after %d retries due to conflict: %w", maxRetries, err)
			}
			return err
		}

		return nil
	}

	return fmt.Errorf("failed to update status after %d attempts", maxRetries)
}

//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dataflow.dataflow.io,resources=dataflows/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *DataFlowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var dataflow dataflowv1.DataFlow
	if err := r.Get(ctx, req.NamespacedName, &dataflow); err != nil {
		log.Error(err, "unable to fetch DataFlow")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	key := fmt.Sprintf("%s/%s", req.Namespace, req.Name)
	log.Info("Reconciling DataFlow", "name", dataflow.Name, "namespace", dataflow.Namespace)

	// Check if DataFlow is being deleted
	if !dataflow.DeletionTimestamp.IsZero() {
		r.mu.Lock()
		if procCtx, exists := r.processors[key]; exists {
			procCtx.cancel()
			delete(r.processors, key)
		}
		r.mu.Unlock()

		if err := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Stopped"
		}); err != nil {
			log.Error(err, "unable to update DataFlow status")
		}
		return ctrl.Result{}, nil
	}

	// Check if processor already exists and if spec has changed
	// Resolve secrets first to compare resolved specs
	resolvedSpec, err := r.secretResolver.ResolveDataFlowSpec(ctx, req.Namespace, &dataflow.Spec)
	if err != nil {
		log.Error(err, "failed to resolve secrets")
		updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
			df.Status.Phase = "Error"
			df.Status.Message = fmt.Sprintf("Failed to resolve secrets: %v", err)
		})
		if updateErr != nil {
			log.Error(updateErr, "unable to update DataFlow status")
		}
		return ctrl.Result{}, err
	}

	r.mu.RLock()
	procCtx, exists := r.processors[key]
	needsRestart := false
	if exists {
		// Compare current resolved spec with the one used to create the processor
		if !reflect.DeepEqual(procCtx.spec, *resolvedSpec) {
			log.Info("DataFlow spec has changed, restarting processor", "name", dataflow.Name, "namespace", dataflow.Namespace)
			needsRestart = true
		}
	}
	r.mu.RUnlock()

	// If spec changed, stop old processor
	if needsRestart {
		r.mu.Lock()
		if procCtx != nil {
			log.Info("Stopping old processor due to spec change", "name", dataflow.Name, "namespace", dataflow.Namespace)
			procCtx.cancel()
			delete(r.processors, key)
		}
		r.mu.Unlock()
		// Wait a bit for graceful shutdown
		time.Sleep(500 * time.Millisecond)
		exists = false
	}

	if !exists {
		// Resolve secrets before creating processor
		resolvedSpec, err := r.secretResolver.ResolveDataFlowSpec(ctx, req.Namespace, &dataflow.Spec)
		if err != nil {
			log.Error(err, "failed to resolve secrets")
			updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
				df.Status.Phase = "Error"
				df.Status.Message = fmt.Sprintf("Failed to resolve secrets: %v", err)
			})
			if updateErr != nil {
				log.Error(updateErr, "unable to update DataFlow status")
			}
			return ctrl.Result{}, err
		}

		// Create new processor with logger
		proc, err := processor.NewProcessorWithLogger(resolvedSpec, log)
		if err != nil {
			log.Error(err, "failed to create processor")
			updateErr := r.updateStatusWithRetry(ctx, req, func(df *dataflowv1.DataFlow) {
				df.Status.Phase = "Error"
				df.Status.Message = fmt.Sprintf("Failed to create processor: %v", err)
			})
			if updateErr != nil {
				log.Error(updateErr, "unable to update DataFlow status")
			}
			return ctrl.Result{}, err
		}

		// Start processor in background
		procCtx, cancel := context.WithCancel(context.Background())
		r.mu.Lock()
		r.processors[key] = &processorContext{
			processor: proc,
			cancel:    cancel,
			spec:      *resolvedSpec,
		}
		r.mu.Unlock()

		go func() {
			if err := proc.Start(procCtx); err != nil {
				log.Error(err, "processor error")
				r.mu.Lock()
				delete(r.processors, key)
				r.mu.Unlock()

				// Update status with a separate context that won't be canceled
				// Use retry logic to handle transient errors
				updateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				// Retry updating status a few times
				var df dataflowv1.DataFlow
				for i := 0; i < 3; i++ {
					if err := r.Get(updateCtx, req.NamespacedName, &df); err != nil {
						log.Error(err, "unable to fetch DataFlow for status update", "attempt", i+1)
						if i < 2 {
							time.Sleep(1 * time.Second)
							continue
						}
						return
					}

					df.Status.Phase = "Error"
					df.Status.Message = fmt.Sprintf("Processor error: %v", err)
					if updateErr := r.Status().Update(updateCtx, &df); updateErr != nil {
						log.Error(updateErr, "unable to update DataFlow status after processor error", "attempt", i+1)
						if i < 2 {
							time.Sleep(1 * time.Second)
							continue
						}
					} else {
						log.Info("Successfully updated DataFlow status to Error")
						break
					}
				}
			}
		}()

		if needsRestart {
			dataflow.Status.Phase = "Running"
			dataflow.Status.Message = "Processor restarted due to spec change"
		} else {
			dataflow.Status.Phase = "Running"
			dataflow.Status.Message = "Processor started"
		}
	} else {
		// Update stats
		r.mu.RLock()
		procCtx := r.processors[key]
		r.mu.RUnlock()

		if procCtx != nil {
			processedCount, errorCount := procCtx.processor.GetStats()
			dataflow.Status.ProcessedCount = processedCount
			dataflow.Status.ErrorCount = errorCount
			dataflow.Status.LastProcessedTime = &metav1.Time{Time: metav1.Now().Time}
		}
		dataflow.Status.Phase = "Running"
	}

	// Update status with retry logic to handle optimistic locking conflicts
	// Используем отдельный контекст, чтобы избежать проблем с отменой
	updateCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Сохраняем значения статуса перед обновлением
	statusPhase := dataflow.Status.Phase
	statusMessage := dataflow.Status.Message
	statusProcessedCount := dataflow.Status.ProcessedCount
	statusErrorCount := dataflow.Status.ErrorCount
	statusLastProcessedTime := dataflow.Status.LastProcessedTime

	if err := r.updateStatusWithRetry(updateCtx, req, func(df *dataflowv1.DataFlow) {
		df.Status.Phase = statusPhase
		df.Status.Message = statusMessage
		df.Status.ProcessedCount = statusProcessedCount
		df.Status.ErrorCount = statusErrorCount
		df.Status.LastProcessedTime = statusLastProcessedTime
	}); err != nil {
		log.Error(err, "unable to update DataFlow status")
		// Don't return error if context was canceled, just log it
		if err == context.Canceled || err == context.DeadlineExceeded {
			return ctrl.Result{Requeue: true}, nil
		}
		// Если это конфликт, запланируем повторную попытку
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DataFlowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dataflowv1.DataFlow{}).
		Complete(r)
}
