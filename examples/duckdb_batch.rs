use duckdb::{Connection, Result as DuckResult};
use candle_generator::{Trade, Instrument, Pair, MarketType, Side};
use chrono::{Utc, TimeZone};
use std::fs;
use std::path::PathBuf;
use candle_batch_aggregator::formats::duckdb::process_duckdb_batch;
use candle_batch_aggregator::Args;

fn main() -> DuckResult<()> {
    // 1. Генерируем трейды
    let trades = vec![
        (1714000000000i64, "binance", "BTC", "USDT", "Spot", "t1", 50000.0, 0.1, "Buy"),
        (1714000060000i64, "binance", "BTC", "USDT", "Spot", "t2", 50100.0, 0.2, "Sell"),
        (1714000120000i64, "binance", "BTC", "USDT", "Spot", "t3", 50200.0, 0.3, "Buy"),
    ];
    let input_dir = PathBuf::from("duckdb_test_data/BTCUSDT");
    fs::create_dir_all(&input_dir).unwrap();
    let db_path = input_dir.join("trades.db");
    if db_path.exists() { fs::remove_file(&db_path).unwrap(); }
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "CREATE TABLE trades (
            timestamp BIGINT,
            exchange TEXT,
            base_id TEXT,
            quote_id TEXT,
            market_type TEXT,
            id TEXT,
            price DOUBLE,
            amount DOUBLE,
            side TEXT
        );"
    )?;
    let mut stmt = conn.prepare("INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
    for t in &trades {
        stmt.execute((t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8))?;
    }
    println!("Тестовые трейды сохранены в {:?}", db_path);

    // 2. Запускаем batch-агрегацию
    let args = Args {
        input: PathBuf::from("duckdb_test_data"),
        output: Some(PathBuf::from("duckdb_test_out")),
        symbol: "BTCUSDT".to_string(),
        interval: "1,5".to_string(),
        format: "duckdb".to_string(),
        benchmark: false,
        progress: false,
        memory_stats: false,
    };
    candle_batch_aggregator::formats::duckdb::process_duckdb_batch(&args).unwrap();

    // 3. Читаем результат
    let out_path = PathBuf::from("duckdb_test_out/BTCUSDT_m1/trades_m1.csv");
    if out_path.exists() {
        let content = std::fs::read_to_string(&out_path).unwrap();
        println!("\nАгрегированные свечи (m1):\n{}", content);
    } else {
        println!("Файл {:?} не найден", out_path);
    }
    Ok(())
} 