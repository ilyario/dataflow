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

	"github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/sjson"
)

// RemoveTransformer removes specific fields from messages
type RemoveTransformer struct {
	config *v1.RemoveTransformation
}

// NewRemoveTransformer creates a new remove transformer
func NewRemoveTransformer(config *v1.RemoveTransformation) *RemoveTransformer {
	return &RemoveTransformer{
		config: config,
	}
}

// Transform removes the specified fields
func (r *RemoveTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	jsonStr := string(message.Data)

	for _, field := range r.config.Fields {
		var err error
		jsonStr, err = sjson.Delete(jsonStr, field)
		if err != nil {
			// Fallback to manual approach
			var data map[string]interface{}
			if err := json.Unmarshal(message.Data, &data); err != nil {
				return nil, err
			}
			// Simple field removal (doesn't support nested paths)
			delete(data, field)
			newData, _ := json.Marshal(data)
			newMsg := types.NewMessage(newData)
			newMsg.Metadata = message.Metadata
			newMsg.Timestamp = message.Timestamp
			return []*types.Message{newMsg}, nil
		}
	}

	newMsg := types.NewMessage([]byte(jsonStr))
	newMsg.Metadata = message.Metadata
	newMsg.Timestamp = message.Timestamp
	return []*types.Message{newMsg}, nil
}
