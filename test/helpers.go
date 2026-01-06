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

package test

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dataflowv1 "github.com/dataflow-operator/dataflow/api/v1"
)

// NewDataFlow creates a test DataFlow resource
func NewDataFlow(name, namespace string, spec dataflowv1.DataFlowSpec) *dataflowv1.DataFlow {
	return &dataflowv1.DataFlow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: spec,
	}
}

// NewKafkaSourceSpec creates a test Kafka source spec
func NewKafkaSourceSpec(brokers []string, topic string) *dataflowv1.KafkaSourceSpec {
	return &dataflowv1.KafkaSourceSpec{
		Brokers:       brokers,
		Topic:         topic,
		ConsumerGroup: "test-group",
	}
}

// NewPostgreSQLSourceSpec creates a test PostgreSQL source spec
func NewPostgreSQLSourceSpec(connectionString, table string) *dataflowv1.PostgreSQLSourceSpec {
	return &dataflowv1.PostgreSQLSourceSpec{
		ConnectionString: connectionString,
		Table:            table,
	}
}

// NewKafkaSinkSpec creates a test Kafka sink spec
func NewKafkaSinkSpec(brokers []string, topic string) *dataflowv1.KafkaSinkSpec {
	return &dataflowv1.KafkaSinkSpec{
		Brokers: brokers,
		Topic:   topic,
	}
}

// NewPostgreSQLSinkSpec creates a test PostgreSQL sink spec
func NewPostgreSQLSinkSpec(connectionString, table string) *dataflowv1.PostgreSQLSinkSpec {
	return &dataflowv1.PostgreSQLSinkSpec{
		ConnectionString: connectionString,
		Table:            table,
	}
}
