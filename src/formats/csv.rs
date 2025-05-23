use super::super::Args;
use anyhow::Result;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use csv::ReaderBuilder;
use serde::Deserialize;
use candle_generator::{Trade, Instrument, Pair, MarketType, Side};
use std::time::Instant;
use crate::aggregation;
use crate::stats::{ProcessingStats, print_summary};
use chrono::TimeZone;

#[derive(Debug, Deserialize)]
struct CsvTrade {
    timestamp: i64,
    price: f64,
    amount: f64,
    side: String,
    #[serde(default)]
    base: String,
    #[serde(default)]
    quote: String,
    #[serde(default)]
    exchange: String,
}

impl CsvTrade {
    fn to_trade(&self) -> Trade {
        Trade {
            instrument: Instrument {
                pair: Pair {
                    base_id: self.base.clone(),
                    quote_id: self.quote.clone(),
                },
                exchange: self.exchange.clone(),
                market_type: MarketType::Spot,
            },
            id: format!("{}", self.timestamp),
            price: self.price,
            amount: self.amount,
            side: match self.side.to_lowercase().as_str() {
                "buy" => Side::Buy,
                "sell" => Side::Sell,
                _ => Side::Unknown,
            },
            timestamp: chrono::Utc.timestamp_millis_opt(self.timestamp).unwrap(),
        }
    }
}

fn parse_intervals(interval_str: &str) -> Vec<candle_generator::Timeframe> {
    if interval_str.to_uppercase() == "ALL" {
        return vec![candle_generator::Timeframe::m1, candle_generator::Timeframe::m5, candle_generator::Timeframe::m15, candle_generator::Timeframe::m30, candle_generator::Timeframe::h1, candle_generator::Timeframe::h4, candle_generator::Timeframe::d1];
    }
    interval_str
        .split(',')
        .filter_map(|s| match s.trim() {
            "1" => Some(candle_generator::Timeframe::m1),
            "5" => Some(candle_generator::Timeframe::m5),
            "15" => Some(candle_generator::Timeframe::m15),
            "30" => Some(candle_generator::Timeframe::m30),
            "60" => Some(candle_generator::Timeframe::h1),
            "240" => Some(candle_generator::Timeframe::h4),
            "1440" => Some(candle_generator::Timeframe::d1),
            _ => None,
        })
        .collect()
}

pub fn process_csv_batch(args: &Args) -> Result<()> {
    let mut stats = ProcessingStats::new();
    stats.start();
    let intervals = parse_intervals(&args.interval);
    let symbols: Vec<String> = if args.symbol.to_uppercase() == "ALL" {
        fs::read_dir(&args.input)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect()
    } else {
        args.symbol.split(',').map(|s| s.trim().to_string()).collect()
    };
    println!("Batch symbols: {:?}", symbols);
    for symbol in &symbols {
        let symbol_dir = args.input.join(symbol);
        if !symbol_dir.exists() { continue; }
        let files: Vec<_> = fs::read_dir(&symbol_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "csv"))
            .collect();
        println!("\nProcessing symbol: {} ({} files)", symbol, files.len());
        for file_path in files {
            let io_start = Instant::now();
            stats.add_file();
            println!("  File: {:?}", file_path.file_name().unwrap());
            let file = File::open(&file_path)?;
            let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
            let mut trades = Vec::new();
            for result in rdr.deserialize() {
                let csv_trade: CsvTrade = result?;
                trades.push(csv_trade.to_trade());
            }
            stats.io_time += io_start.elapsed();
            stats.add_trades(trades.len());
            println!("    Trades: {}", trades.len());
            let agg_start = Instant::now();
            let chain = aggregation::aggregate_trades_chain(trades.iter(), &intervals);
            stats.aggregation_time += agg_start.elapsed();
            for (tf, candles) in chain {
                stats.add_candles(&format!("{:?}", tf), candles.len());
                let out_dir = args.output.clone().unwrap_or_else(|| PathBuf::from("candles"));
                let out_dir = out_dir.join(format!("{}_{}", symbol, format!("{:?}", tf)));
                fs::create_dir_all(&out_dir)?;
                let out_file = out_dir.join(format!("{}_{}.csv", file_path.file_stem().unwrap().to_string_lossy(), format!("{:?}", tf)));
                let io_start = Instant::now();
                aggregation::write_candles_csv(&candles, &out_file)?;
                stats.io_time += io_start.elapsed();
                println!("    [{:?}] Candles: {} -> {:?}", tf, candles.len(), out_file);
            }
        }
    }
    stats.stop();
    print_summary(&stats);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_csv_trade_parsing() {
        let data = "timestamp,price,amount,side,base,quote,exchange\n\
                    1714000000000,50000.0,0.1,buy,BTC,USDT,binance\n";
        let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(Cursor::new(data));
        let mut trades = Vec::new();
        for result in rdr.deserialize() {
            let csv_trade: CsvTrade = result.unwrap();
            let trade = csv_trade.to_trade();
            trades.push(trade);
        }
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price, 50000.0);
        assert_eq!(trades[0].amount, 0.1);
        assert_eq!(trades[0].instrument.pair.base_id, "BTC");
        assert_eq!(trades[0].side, candle_generator::Side::Buy);
    }

    #[test]
    fn test_csv_empty() {
        let data = "timestamp,price,amount,side,base,quote,exchange\n";
        let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(Cursor::new(data));
        let mut trades = Vec::new();
        for result in rdr.deserialize() {
            let csv_trade: Result<CsvTrade, _> = result;
            assert!(csv_trade.is_ok());
        }
        assert_eq!(trades.len(), 0);
    }

    #[test]
    fn test_csv_incorrect_data() {
        let data = "timestamp,price,amount,side,base,quote,exchange\n\
                    not_a_timestamp,not_a_price,not_an_amount,side,BTC,USDT,binance\n";
        let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(Cursor::new(data));
        let mut errors = 0;
        for result in rdr.deserialize() {
            if result.is_err() { errors += 1; }
        }
        assert_eq!(errors, 1);
    }

    #[test]
    fn test_csv_multiple_symbols() {
        let data = "timestamp,price,amount,side,base,quote,exchange\n\
                    1714000000000,50000.0,0.1,buy,BTC,USDT,binance\n\
                    1714000001000,3000.0,0.5,sell,ETH,USDT,binance\n";
        let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(Cursor::new(data));
        let mut trades = Vec::new();
        for result in rdr.deserialize() {
            let csv_trade: CsvTrade = result.unwrap();
            trades.push(csv_trade.to_trade());
        }
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].instrument.pair.base_id, "BTC");
        assert_eq!(trades[1].instrument.pair.base_id, "ETH");
    }

    #[test]
    fn test_parse_intervals() {
        let tfs = parse_intervals("1,5,15");
        assert_eq!(tfs.len(), 3);
        let tfs_all = parse_intervals("ALL");
        assert!(tfs_all.len() > 3);
    }
} 