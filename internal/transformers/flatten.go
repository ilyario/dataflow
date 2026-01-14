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

package transformers

import (
	"context"
	"encoding/json"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
)

// FlattenTransformer flattens an array field into multiple messages
type FlattenTransformer struct {
	config *v1.FlattenTransformation
}

// NewFlattenTransformer creates a new flatten transformer
func NewFlattenTransformer(config *v1.FlattenTransformation) *FlattenTransformer {
	return &FlattenTransformer{
		config: config,
	}
}

// Transform flattens the array field into multiple messages
func (f *FlattenTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	// Get the array field using JSONPath
	result := gjson.GetBytes(message.Data, f.config.Field)
	if !result.Exists() {
		// Field doesn't exist, return original message
		return []*types.Message{message}, nil
	}

	if !result.IsArray() {
		// Field is not an array, return original message
		return []*types.Message{message}, nil
	}

	// Parse the original message
	var originalData map[string]interface{}
	if err := json.Unmarshal(message.Data, &originalData); err != nil {
		// Если данные не являются валидным JSON, возвращаем исходное сообщение без изменений
		// Это позволяет обрабатывать бинарные данные или другие форматы
		return []*types.Message{message}, nil
	}

	// Get the array
	array := result.Array()
	if len(array) == 0 {
		// Empty array, return original message without the array field
		delete(originalData, f.config.Field)
		newData, _ := json.Marshal(originalData)
		newMsg := types.NewMessage(newData)
		newMsg.Metadata = message.Metadata
		newMsg.Timestamp = message.Timestamp
		return []*types.Message{newMsg}, nil
	}

	// Create a message for each element in the array
	messages := make([]*types.Message, 0, len(array))
	for _, item := range array {
		// Create a copy of the original data
		newData := make(map[string]interface{})
		for k, v := range originalData {
			if k != f.config.Field {
				newData[k] = v
			}
		}

		// Add the flattened item fields to the root
		if item.IsObject() {
			itemMap := item.Map()
			for k, v := range itemMap {
				newData[k] = v.Value()
			}
		} else {
			// If it's a primitive, add it with the field name
			newData[f.config.Field] = item.Value()
		}

		jsonData, err := json.Marshal(newData)
		if err != nil {
			continue
		}

		newMsg := types.NewMessage(jsonData)
		newMsg.Metadata = message.Metadata
		newMsg.Timestamp = message.Timestamp
		messages = append(messages, newMsg)
	}

	return messages, nil
}
