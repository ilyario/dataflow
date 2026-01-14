# pkg/

Эта директория содержит внешние библиотеки, которые используются в проекте.

## nessie-client

Go API клиент для Nessie (Transactional Catalog for Data Lakes), сгенерированный из OpenAPI спецификации.

### Использование

```go
import "github.com/dataflow-operator/dataflow/pkg/nessie-client"
```

Библиотека настроена через `replace` директиву в `go.mod`:

```go
replace github.com/dataflow-operator/dataflow/pkg/nessie-client => ./pkg/nessie-client
```

### Документация

См. [README.md](./nessie-client/README.md) для подробной документации по использованию API клиента.
