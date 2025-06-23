#!/usr/bin/env python3
"""
Elasticsearch CLI - Файл обратной совместимости
Импортирует из модульной структуры для сохранения совместимости
"""

from elastic_cli.cli import ElasticsearchCLI, main

if __name__ == "__main__":
    main()
