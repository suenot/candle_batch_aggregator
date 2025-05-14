use polars::prelude::*;
use candle_generator::{Trade, Instrument, Pair, MarketType, Side, Candle};
use chrono::{Utc, TimeZone};
use std::fs;
use std::path::PathBuf;
use candle_batch_aggregator::formats::parquet::process_parquet_batch;
use candle_batch_aggregator::Args;

fn main() -> polars::prelude::PolarsResult<()> {
    // 1. Генерируем трейды
    let trades = vec![
        (1714000000000i64, "binance", "BTC", "USDT", "Spot", "t1", 50000.0, 0.1, "Buy"),
        (1714000060000i64, "binance", "BTC", "USDT", "Spot", "t2", 50100.0, 0.2, "Sell"),
        (1714000120000i64, "binance", "BTC", "USDT", "Spot", "t3", 50200.0, 0.3, "Buy"),
    ];
    let timestamps: Vec<_> = trades.iter().map(|t| t.0).collect();
    let exchanges: Vec<_> = trades.iter().map(|t| t.1.to_string()).collect();
    let base_ids: Vec<_> = trades.iter().map(|t| t.2.to_string()).collect();
    let quote_ids: Vec<_> = trades.iter().map(|t| t.3.to_string()).collect();
    let market_types: Vec<_> = trades.iter().map(|t| t.4.to_string()).collect();
    let ids: Vec<_> = trades.iter().map(|t| t.5.to_string()).collect();
    let prices: Vec<_> = trades.iter().map(|t| t.6).collect();
    let amounts: Vec<_> = trades.iter().map(|t| t.7).collect();
    let sides: Vec<_> = trades.iter().map(|t| t.8.to_string()).collect();
    let df = DataFrame::new(vec![
        Series::new("timestamp", timestamps),
        Series::new("exchange", exchanges),
        Series::new("base_id", base_ids),
        Series::new("quote_id", quote_ids),
        Series::new("market_type", market_types),
        Series::new("id", ids),
        Series::new("price", prices),
        Series::new("amount", amounts),
        Series::new("side", sides),
    ])?;
    let input_dir = PathBuf::from("parquet_test_data/BTCUSDT");
    fs::create_dir_all(&input_dir).unwrap();
    let parquet_path = input_dir.join("trades.parquet");
    df.write_parquet(&parquet_path, ParquetWriteOptions::default())?;
    println!("Тестовые трейды сохранены в {:?}", parquet_path);

    // 2. Запускаем batch-агрегацию
    let args = Args {
        input: PathBuf::from("parquet_test_data"),
        output: Some(PathBuf::from("parquet_test_out")),
        symbol: "BTCUSDT".to_string(),
        interval: "1,5".to_string(),
        format: "parquet".to_string(),
        benchmark: false,
        progress: false,
        memory_stats: false,
    };
    candle_batch_aggregator::formats::parquet::process_parquet_batch(&args).unwrap();

    // 3. Читаем результат
    let out_path = PathBuf::from("parquet_test_out/BTCUSDT_m1/trades_m1.parquet");
    if out_path.exists() {
        let out_df = LazyFrame::scan_parquet(&out_path.to_string_lossy(), Default::default())?.collect()?;
        println!("\nАгрегированные свечи (m1):");
        println!("{}", out_df);
    } else {
        println!("Файл {:?} не найден", out_path);
    }
    Ok(())
} 