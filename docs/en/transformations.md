# Transformations

DataFlow Operator supports various message transformations that are applied sequentially to each message in the order specified in the configuration. All transformations support JSONPath for working with nested data structures.

> **Note**: This is a simplified English version. For complete documentation, see the [Russian version](../ru/transformations.md).

## Transformation Overview

| Transformation | Description | Input | Output |
|----------------|------------|-------|--------|
| Timestamp | Adds timestamp | 1 message | 1 message |
| Flatten | Expands arrays | 1 message | N messages |
| Filter | Filters by condition | 1 message | 0 or 1 message |
| Mask | Masks data | 1 message | 1 message |
| Router | Routes to different sinks | 1 message | 0 or 1 message |
| Select | Selects fields | 1 message | 1 message |
| Remove | Removes fields | 1 message | 1 message |

## Timestamp

Adds a timestamp field to each message. Useful for tracking message processing time.

### Configuration

```yaml
transformations:
  - type: timestamp
    timestamp:
      # Field name for timestamp (optional, default: created_at)
      fieldName: created_at
      # Timestamp format (optional, default: RFC3339)
      format: RFC3339
```

### Supported Formats

- `RFC3339` - `2006-01-02T15:04:05Z07:00` (default)
- `RFC3339Nano` - `2006-01-02T15:04:05.999999999Z07:00`
- `Unix` - Unix timestamp in seconds
- `UnixMilli` - Unix timestamp in milliseconds
- Any custom Go time package format

## Flatten

Expands an array into separate messages, preserving all parent fields. Useful for processing nested data structures.

### Configuration

```yaml
transformations:
  - type: flatten
    flatten:
      # JSONPath to array to expand (required)
      field: items
```

## Filter

Filters messages based on JSONPath conditions. Messages that don't match the condition are removed from the stream.

### Configuration

```yaml
transformations:
  - type: filter
    filter:
      # JSONPath expression that must be true (required)
      condition: "$.active"
```

## Mask

Masks sensitive data in specified fields. Supports preserving length or full character replacement.

### Configuration

```yaml
transformations:
  - type: mask
    mask:
      # List of JSONPath expressions to fields to mask (required)
      fields:
        - password
        - email
      # Character for masking (optional, default: *)
      maskChar: "*"
      # Preserve original length (optional, default: false)
      keepLength: true
```

## Router

Routes messages to different sinks based on conditions. Messages matching a condition are sent to the specified sink instead of the main one.

### Configuration

```yaml
transformations:
  - type: router
    router:
      routes:
        - condition: "$.level"
          sink:
            type: kafka
            kafka:
              brokers: ["localhost:9092"]
              topic: error-topic
```

## Select

Selects only specified fields from a message. Useful for reducing data size and improving performance.

### Configuration

```yaml
transformations:
  - type: select
    select:
      # List of JSONPath expressions to fields to select (required)
      fields:
        - id
        - name
        - email
```

## Remove

Removes specified fields from a message. Useful for data cleanup before sending.

### Configuration

```yaml
transformations:
  - type: remove
    remove:
      # List of JSONPath expressions to fields to remove (required)
      fields:
        - password
        - internal_id
```

## Order of Application

Transformations are applied sequentially in the order specified in the `transformations` list. Each transformation receives the result of the previous one.

### Recommended Order

1. **Flatten** should be first if you need to expand arrays
2. **Filter** apply early to reduce the volume of processed data
3. **Mask/Remove** apply before Select for security
4. **Select** apply at the end for final cleanup
5. **Timestamp** can be applied anywhere, but usually at the beginning or end
6. **Router** usually applied at the end, after all other transformations

For complete transformation documentation with examples, see the [Russian version](../ru/transformations.md).

