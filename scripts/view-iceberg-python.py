#!/usr/bin/env python3
"""
Скрипт для просмотра данных из Iceberg таблицы с использованием pyiceberg
Требует: pip install pyiceberg pyarrow s3fs

Настройки доступа к MinIO можно задать через переменные окружения:
  MINIO_ENDPOINT - адрес MinIO (по умолчанию: http://localhost:9000)
  AWS_ACCESS_KEY_ID - ключ доступа (по умолчанию: admin)
  AWS_SECRET_ACCESS_KEY - секретный ключ (по умолчанию: password)
  AWS_REGION - регион (по умолчанию: us-east-1)
"""

import sys
import os

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NoSuchTableError
    # Настройка pyarrow для работы с MinIO
    try:
        import pyarrow.fs as pafs
        # Настройка S3FileSystem для MinIO
        # Это нужно для того, чтобы pyarrow правильно работал с MinIO
    except ImportError:
        pass
except ImportError:
    print("ОШИБКА: pyiceberg не установлен")
    print("Установите: pip install pyiceberg pyarrow s3fs")
    sys.exit(1)

def main():
    rest_url = os.getenv("ICEBERG_REST_URL", "http://localhost:8081")
    namespace = os.getenv("ICEBERG_NAMESPACE", "default")
    table_name = os.getenv("ICEBERG_TABLE", "stock_items")

    # Настройки для доступа к MinIO (S3-совместимое хранилище)
    # Эти переменные нужны для pyiceberg, который использует AWS SDK для доступа к файлам
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    # Установка переменных окружения для AWS SDK (используется pyiceberg)
    # PyIceberg использует pyarrow и fsspec для доступа к S3/MinIO
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key
    os.environ["AWS_REGION"] = aws_region
    # Для boto3/pyarrow используется AWS_ENDPOINT_URL_S3
    os.environ["AWS_ENDPOINT_URL_S3"] = minio_endpoint
    # Альтернативные переменные для разных библиотек
    os.environ["S3_ENDPOINT"] = minio_endpoint
    # Для fsspec/s3fs (используется pyiceberg через pyarrow)
    os.environ["S3_ENDPOINT_URL"] = minio_endpoint
    # Для s3fs напрямую
    os.environ["S3FS_ENDPOINT_URL"] = minio_endpoint
    # Отключение SSL для локального MinIO
    if minio_endpoint.startswith("http://"):
        os.environ["AWS_CA_BUNDLE"] = ""
        os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
        # Для pyarrow.fs нужно явно указать, что используется HTTP
        os.environ["AWS_USE_SSL"] = "false"
        os.environ["AWS_VERIFY_SSL"] = "false"

    # Дополнительные настройки для s3fs (используется pyarrow)
    # s3fs использует endpoint_url для настройки MinIO
    os.environ["ENDPOINT_URL"] = minio_endpoint

    print(f"Подключение к Iceberg REST Catalog: {rest_url}")
    print(f"Таблица: {namespace}.{table_name}")
    print(f"MinIO Endpoint: {minio_endpoint}")
    print()

    try:
        # Загрузка каталога с явной настройкой S3FileIO для MinIO
        # PyIceberg требует явной настройки S3FileIO через properties
        # Используем свойства с точками (стандартный формат Iceberg)
        catalog_properties = {
            "type": "rest",
            "uri": rest_url,
            # Настройки S3FileIO для MinIO (используем точки в именах свойств)
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.endpoint": minio_endpoint,
            "s3.access-key-id": aws_access_key,
            "s3.secret-access-key": aws_secret_key,
            "s3.region": aws_region,
            "s3.path-style-access": "true",
        }

        catalog = load_catalog(
            name="iceberg",
            **catalog_properties
        )

        # Загрузка таблицы
        full_table_name = f"{namespace}.{table_name}"
        print(f"Загрузка таблицы: {full_table_name}")
        table = catalog.load_table(full_table_name)

        # Получение схемы
        print("\n=== Схема таблицы ===")
        for field in table.schema().fields:
            print(f"  {field.name}: {field.field_type}")

        # Чтение данных
        print(f"\n=== Данные таблицы ===")
        scan = table.scan()
        arrow_table = scan.to_arrow()

        row_count = len(arrow_table)
        print(f"Найдено записей: {row_count}\n")

        if row_count > 0:
            # Конвертация в список словарей для удобного вывода
            import json
            data = arrow_table.to_pylist()

            # Вывод первых 10 записей
            max_rows = min(10, row_count)
            print(f"Показываю первые {max_rows} записей:\n")
            for i, row in enumerate(data[:max_rows], 1):
                print(f"Запись {i}:")
                print(json.dumps(row, indent=2, ensure_ascii=False))
                print()

            if row_count > max_rows:
                print(f"... и еще {row_count - max_rows} записей")
        else:
            print("Таблица пуста")

    except NoSuchTableError:
        print(f"ОШИБКА: Таблица {namespace}.{table_name} не найдена")
        print("Убедитесь, что таблица создана и существует")
        sys.exit(1)
    except Exception as e:
        print(f"ОШИБКА: {type(e).__name__}: {e}")
        print("\nИспользуемые настройки:")
        print(f"  AWS_ACCESS_KEY_ID: {aws_access_key}")
        print(f"  AWS_SECRET_ACCESS_KEY: {'*' * len(aws_secret_key)}")
        print(f"  AWS_REGION: {aws_region}")
        print(f"  AWS_ENDPOINT_URL_S3: {minio_endpoint}")
        print("\nВозможные причины:")
        print("1. Iceberg REST API недоступен")
        print("2. Таблица не существует")
        print("3. Проблемы с доступом к MinIO (проверьте endpoint и credentials)")
        print("4. MinIO недоступен по адресу:", minio_endpoint)
        print("\nДля проверки MinIO:")
        print("  docker-compose ps minio")
        print("  curl http://localhost:9000/minio/health/live")
        sys.exit(1)

if __name__ == "__main__":
    main()

