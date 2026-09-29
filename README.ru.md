[English](README.md) | [Русский](README.ru.md)

# Data Engineering Tech Radar

Pet-проект для отслеживания активности open-source технологий в области Data Engineering.

Проект загружает публичные события GitHub из GH Archive, хранит исторические данные в lakehouse, преобразует их в аналитические витрины и визуализирует тренды технологий в Tableau.

## Цель

Определить, какие технологии Data Engineering набирают или теряют популярность на основе наблюдаемой активности на GitHub.

## Текущий функционал

Пайплайн загружает один часовой файл GH Archive в локальную Raw-зону и записывает его события в Bronze-таблицу Apache Iceberg.

```text
GH Archive
  → потоковая HTTP-загрузка
  → партиционированный Raw .json.gz
  → потоковый парсер
  → ограниченные PyArrow-батчи
  → Bronze-таблица Apache Iceberg
```

## Быстрый старт

Требования:

- Python 3.14
- uv

Установка проекта и зафиксированных зависимостей:

```bash
uv sync --locked
```

Загрузка одного часового архива:

```bash
uv run de-tech-radar ingest-gharchive \
  --hour 2015-01-01T15:00:00Z \
  --raw-root data/raw
```

Результат:

```text
data/raw/gharchive/archive_date=2015-01-01/archive_hour=15/2015-01-01-15.json.gz
```

В `--hour` необходимо передать точный час с часовым поясом. `Z` означает UTC. Уже существующие Raw-файлы повторно не загружаются. Локальная директория `data/` исключена из Git.

Загрузка скачанного архива в локальную Bronze-таблицу Iceberg:

```bash
uv run de-tech-radar load-gharchive-bronze \
  --archive-path data/raw/gharchive/archive_date=2015-01-01/archive_hour=15/2015-01-01-15.json.gz \
  --hour 2015-01-01T15:00:00Z
```

Локальный lakehouse для разработки использует SQLite-каталог `data/lakehouse/catalog.db`. Iceberg metadata и Parquet-файлы сохраняются в `data/lakehouse/warehouse`.

Команда выводит количество записанных событий. Успешно обработанный исходный файл отмечается в metadata Iceberg snapshot, поэтому повторный запуск возвращает `0`. Все батчи одного архива публикуются атомарно.
