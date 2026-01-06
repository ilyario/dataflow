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

package connectors

import (
	"context"
	"fmt"
	"sync"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQSourceConnector implements SourceConnector for RabbitMQ
type RabbitMQSourceConnector struct {
	config  *v1.RabbitMQSourceSpec
	conn    *amqp.Connection
	channel *amqp.Channel
	closed  bool
	mu      sync.Mutex
}

// NewRabbitMQSourceConnector creates a new RabbitMQ source connector
func NewRabbitMQSourceConnector(config *v1.RabbitMQSourceSpec) *RabbitMQSourceConnector {
	return &RabbitMQSourceConnector{
		config: config,
	}
}

// Connect establishes connection to RabbitMQ
func (r *RabbitMQSourceConnector) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("connector is closed")
	}

	conn, err := amqp.Dial(r.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	r.conn = conn
	r.channel = ch
	return nil
}

// Read returns a channel of messages from RabbitMQ
func (r *RabbitMQSourceConnector) Read(ctx context.Context) (<-chan *types.Message, error) {
	if r.channel == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	// Declare queue if needed
	_, err := r.channel.QueueDeclare(
		r.config.Queue,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// Bind queue to exchange if exchange is provided
	if r.config.Exchange != "" {
		err = r.channel.QueueBind(
			r.config.Queue,
			r.config.RoutingKey,
			r.config.Exchange,
			false,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to bind queue: %w", err)
		}
	}

	msgs, err := r.channel.Consume(
		r.config.Queue,
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}

	msgChan := make(chan *types.Message, 100)

	go func() {
		defer close(msgChan)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}
				msg := types.NewMessage(d.Body)
				msg.Metadata["exchange"] = d.Exchange
				msg.Metadata["routingKey"] = d.RoutingKey
				msg.Metadata["deliveryTag"] = d.DeliveryTag

				select {
				case msgChan <- msg:
					d.Ack(false)
				case <-ctx.Done():
					d.Nack(false, true)
					return
				}
			}
		}
	}()

	return msgChan, nil
}

// Close closes the RabbitMQ connection
func (r *RabbitMQSourceConnector) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// RabbitMQSinkConnector implements SinkConnector for RabbitMQ
type RabbitMQSinkConnector struct {
	config  *v1.RabbitMQSinkSpec
	conn    *amqp.Connection
	channel *amqp.Channel
	closed  bool
	mu      sync.Mutex
}

// NewRabbitMQSinkConnector creates a new RabbitMQ sink connector
func NewRabbitMQSinkConnector(config *v1.RabbitMQSinkSpec) *RabbitMQSinkConnector {
	return &RabbitMQSinkConnector{
		config: config,
	}
}

// Connect establishes connection to RabbitMQ
func (r *RabbitMQSinkConnector) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("connector is closed")
	}

	conn, err := amqp.Dial(r.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare exchange
	err = ch.ExchangeDeclare(
		r.config.Exchange,
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	r.conn = conn
	r.channel = ch
	return nil
}

// Write writes messages to RabbitMQ
func (r *RabbitMQSinkConnector) Write(ctx context.Context, messages <-chan *types.Message) error {
	if r.channel == nil {
		return fmt.Errorf("not connected, call Connect first")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return nil
			}

			routingKey := r.config.RoutingKey
			if rk, ok := msg.Metadata["routingKey"].(string); ok {
				routingKey = rk
			}

			err := r.channel.Publish(
				r.config.Exchange,
				routingKey,
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        msg.Data,
				},
			)
			if err != nil {
				return fmt.Errorf("failed to publish message: %w", err)
			}
		}
	}
}

// Close closes the RabbitMQ connection
func (r *RabbitMQSinkConnector) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
