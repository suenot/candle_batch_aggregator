# candle_batch_aggregator

**CLI-инструмент для пакетной агрегации свечей из трейдов с поддержкой CSV, Parquet, DuckDB, QuestDB, ClickHouse и других форматов.**

---

## Возможности
- Пакетная обработка исторических данных по файлам/папкам
- Поддержка CSV (MVP), Parquet, DuckDB, QuestDB, ClickHouse (адаптеры)
- Гибкая система флагов (input/output, symbol, interval, format, benchmark, progress и др.)
- Агрегация цепочкой: младшие таймфреймы из трейдов, старшие — из младших свечей
- Прогресс, бенчмаркинг, автоматизация, расширяемость
- Интеграция с ядром candle_generator (унификация структуры Trade/Candle)

---

## Пример запуска

```bash
cargo run -p candle_batch_aggregator -- -i ./data -s BTCUSDT,ETHUSDT -t 1,5,15,60 -f csv -b -p
```

---

## Флаги CLI
- `-i, --input <PATH>`: директория с историческими файлами (CSV, Parquet, ...)
- `-o, --output <PATH>`: директория для свечей (по умолчанию ../candles)
- `-s, --symbol <SYMBOLS>`: пары (через запятую) или ALL
- `-t, --interval <INTERVALS>`: таймфреймы (через запятую или ALL)
- `-f, --format <FORMAT>`: формат входных файлов (csv/parquet/duckdb/questdb/clickhouse/auto)
- `-b, --benchmark`: подробные метрики
- `-p, --progress`: прогресс
- `-m, --memory-stats`: статистика по памяти

---

## Архитектура
- src/main.rs: точка входа, парсинг флагов, диспетчеризация по формату
- src/formats/: модули-адаптеры для чтения трейдов из разных форматов
- src/aggregation.rs: универсальная логика агрегации трейдов в свечи через candle_generator
- src/chain.rs: агрегация цепочкой (из младших свечей в старшие)
- src/stats.rs: сбор и вывод метрик, прогресс, бенчмаркинг

---

## Расширяемость
- Для добавления нового формата — реализуйте модуль в src/formats/ и зарегистрируйте его в main.rs
- Для новых метрик — расширяйте ядро candle_generator и используйте кастомные CandleMetric

---

## MVP: поддержка CSV
- На первом этапе реализована поддержка CSV (как в примере bybit-generate-candles)
- После этого — добавление Parquet, DuckDB, QuestDB, ClickHouse

---

## Примеры запуска

```bash
# Обработка всех пар и всех таймфреймов из CSV
candle-batch-aggregator -i ./data -s ALL -t ALL -f csv -b -p

# Обработка одной пары и нескольких таймфреймов
candle-batch-aggregator -i ./data -s BTCUSDT -t 1,5,15,60 -f csv
```

---

## TODO
- [ ] Поддержка Parquet, DuckDB, QuestDB, ClickHouse
- [ ] Расширяемые метрики через CandleMetric
- [ ] Интеграция с CI и автоматизация тестов 