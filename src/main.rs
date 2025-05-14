mod aggregation;
mod stats;
mod chain;
mod formats {
    pub mod csv;
    pub mod parquet;
    pub mod duckdb;
    pub mod questdb;
    pub mod clickhouse;
}

use clap::Parser;
use std::path::PathBuf;
use anyhow::Result;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Root directory containing trade history files
    #[arg(short = 'i', long)]
    input: PathBuf,

    /// Output directory for candles (optional)
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,

    /// Trading pair symbols (comma-separated) or "ALL" for all
    #[arg(short = 's', long)]
    symbol: String,

    /// Candle intervals in minutes (comma-separated or "ALL")
    #[arg(short = 't', long, default_value = "1")]
    interval: String,

    /// Input format (csv/parquet/duckdb/questdb/clickhouse/auto)
    #[arg(short = 'f', long, default_value = "csv")]
    format: String,

    /// Enable detailed performance metrics
    #[arg(short = 'b', long)]
    benchmark: bool,

    /// Show progress for each file
    #[arg(short = 'p', long)]
    progress: bool,

    /// Print memory usage statistics
    #[arg(short = 'm', long)]
    memory_stats: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    // Поддержка разных форматов
    match args.format.as_str() {
        "csv" => formats::csv::process_csv_batch(&args)?,
        "parquet" => formats::parquet::process_parquet_batch(&args)?,
        "duckdb" => formats::duckdb::process_duckdb_batch(&args)?,
        "questdb" => formats::questdb::process_questdb_batch(&args)?,
        "clickhouse" => formats::clickhouse::process_clickhouse_batch(&args)?,
        _ => {
            eprintln!("Only CSV, Parquet, DuckDB, QuestDB and ClickHouse formats supported in MVP");
            std::process::exit(1);
        }
    }
    Ok(())
}
