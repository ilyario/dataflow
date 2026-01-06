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
	"fmt"
	"strings"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/types"
	"github.com/tidwall/gjson"
)

// RouterTransformer routes messages to different sinks based on conditions
type RouterTransformer struct {
	config *v1.RouterTransformation
}

// NewRouterTransformer creates a new router transformer
func NewRouterTransformer(config *v1.RouterTransformation) *RouterTransformer {
	return &RouterTransformer{
		config: config,
	}
}

// Transform routes messages based on conditions
// Returns messages with routing metadata
func (r *RouterTransformer) Transform(ctx context.Context, message *types.Message) ([]*types.Message, error) {
	fmt.Printf("DEBUG Router: Processing message, routes count: %d\n", len(r.config.Routes))
	fmt.Printf("DEBUG Router: Message data: %s\n", string(message.Data))

	for i, route := range r.config.Routes {
		// Check if condition contains comparison operator (==)
		condition := route.Condition
		var fieldPath string
		var expectedValue string
		var isComparison bool

		fmt.Printf("DEBUG Router: Checking route %d, condition: '%s'\n", i, condition)

		// Parse condition like "$.type == 'order'" or "$.type"
		if idx := findComparisonOperator(condition); idx >= 0 {
			// Trim spaces from field path
			fieldPath = strings.TrimSpace(condition[:idx])
			expectedValue = extractStringValue(condition[idx:])
			isComparison = true
			fmt.Printf("DEBUG Router: Parsed comparison - fieldPath: '%s', expectedValue: '%s'\n", fieldPath, expectedValue)
		} else {
			fieldPath = condition
			isComparison = false
			fmt.Printf("DEBUG Router: No comparison operator found, using fieldPath: '%s'\n", fieldPath)
		}

		// Remove $. prefix if present (gjson doesn't need it for root fields)
		if strings.HasPrefix(fieldPath, "$.") {
			fieldPath = fieldPath[2:]
		} else if strings.HasPrefix(fieldPath, "$") {
			fieldPath = fieldPath[1:]
		}

		// Evaluate the condition
		result := gjson.GetBytes(message.Data, fieldPath)

		if !result.Exists() {
			fmt.Printf("DEBUG Router: Field '%s' does not exist in message\n", fieldPath)
			continue
		}

		fmt.Printf("DEBUG Router: Field '%s' exists, value: '%s' (raw: %v)\n", fieldPath, result.String(), result.Value())

		// Check if condition is true
		var isTrue bool
		if isComparison {
			// For comparison, check if value matches expected
			value := result.String()
			isTrue = value == expectedValue
			fmt.Printf("DEBUG Router: Comparison result: value='%s' == expected='%s' = %v\n", value, expectedValue, isTrue)
		} else {
			// For simple existence check, use truthiness
			value := result.Value()
			switch v := value.(type) {
			case bool:
				isTrue = v
			case string:
				isTrue = v != "" && v != "false"
			case float64:
				isTrue = v != 0
			case nil:
				isTrue = false
			default:
				isTrue = true
			}
		}

		if isTrue {
			// Add routing metadata to message (store condition as key for routing)
			newMsg := types.NewMessage(message.Data)
			newMsg.Metadata = make(map[string]interface{})
			for k, v := range message.Metadata {
				newMsg.Metadata[k] = v
			}
			newMsg.Metadata["routed_condition"] = route.Condition
			newMsg.Timestamp = message.Timestamp
			// Debug: log routing decision
			fmt.Printf("DEBUG Router: Message routed to condition '%s', value='%s', expected='%s'\n",
				route.Condition, result.String(), expectedValue)
			return []*types.Message{newMsg}, nil
		} else if isComparison {
			// Debug: log why condition didn't match
			fmt.Printf("DEBUG Router: Condition '%s' didn't match: value='%s', expected='%s'\n",
				route.Condition, result.String(), expectedValue)
		}
	}

	// No route matched, return original message
	return []*types.Message{message}, nil
}

// findComparisonOperator finds the position of "==" operator in condition string
// Returns the position of the first '=' character, or -1 if not found
func findComparisonOperator(condition string) int {
	// Look for " == " with spaces around it (most common format)
	for i := 0; i < len(condition)-2; i++ {
		if condition[i] == ' ' && condition[i+1] == '=' && condition[i+2] == '=' {
			// Found " ==", check if there's a space or quote after
			if i+3 < len(condition) && (condition[i+3] == ' ' || condition[i+3] == '\'' || condition[i+3] == '"') {
				return i + 1 // Return position of first '='
			}
		}
	}
	// Also check for "==" without leading space but with trailing space/quote
	for i := 1; i < len(condition)-1; i++ {
		if condition[i] == '=' && condition[i+1] == '=' {
			// Check if before is end of field path and after is space or quote
			beforeOK := condition[i-1] != '=' // Not part of another ==
			afterOK := i+2 < len(condition) && (condition[i+2] == ' ' || condition[i+2] == '\'' || condition[i+2] == '"')
			if beforeOK && afterOK {
				return i
			}
		}
	}
	return -1
}

// extractStringValue extracts string value from comparison like " == 'value'" or ' == "value"'
func extractStringValue(comparison string) string {
	// Remove " == " prefix
	comparison = strings.TrimSpace(comparison)
	if !strings.HasPrefix(comparison, "==") {
		return ""
	}
	// Remove "==" and spaces
	comparison = strings.TrimPrefix(comparison, "==")
	comparison = strings.TrimSpace(comparison)

	// Extract quoted value
	if len(comparison) > 0 {
		quote := comparison[0]
		if quote == '\'' || quote == '"' {
			// Find closing quote (handle escaped quotes)
			for i := 1; i < len(comparison); i++ {
				if comparison[i] == quote && (i == 1 || comparison[i-1] != '\\') {
					return comparison[1:i]
				}
			}
		}
	}
	return ""
}
