use anyhow::Result;
use candle_generator::{Candle, Timeframe, CandleGenerator, Trade};
use std::collections::HashMap;
use std::path::Path;
use csv::WriterBuilder;
use serde::Serialize;
use chrono::{DateTime, Utc, Timelike};

pub fn aggregate_trades_to_candles(/* trades, interval, ... */) -> Result<()> {
    // TODO: реализовать агрегацию через candle_generator
    Ok(())
}

fn aggregate_from_lower(lower: &[Candle], tf: &Timeframe) -> Vec<Candle> {
    let count = match tf {
        Timeframe::m5 => 5,
        Timeframe::m15 => 3,
        Timeframe::m30 => 2,
        Timeframe::h1 => 2,
        Timeframe::h4 => 4,
        Timeframe::d1 => 6,
        _ => 1,
    };
    let mut result = Vec::new();
    let mut i = 0;
    while i + count <= lower.len() {
        let slice = &lower[i..i+count];
        let open = slice.first().unwrap().open;
        let close = slice.last().unwrap().close;
        let high = slice.iter().map(|c| c.high).fold(f64::MIN, f64::max);
        let low = slice.iter().map(|c| c.low).fold(f64::MAX, f64::min);
        let volume = slice.iter().map(|c| c.volume).sum();
        let trade_count = slice.iter().map(|c| c.trade_count).sum();
        let volume_usdt = if slice.iter().all(|c| c.volume_usdt.is_some()) {
            Some(slice.iter().map(|c| c.volume_usdt.unwrap()).sum())
        } else {
            None
        };
        let candle = Candle {
            instrument: slice[0].instrument.clone(),
            interval: tf.clone(),
            timestamp: slice[0].timestamp, // или truncate_to_tf(slice[0].timestamp, tf)
            open, high, low, close, volume, trade_count, volume_usdt,
            custom: std::collections::HashMap::new(),
        };
        i += count;
        result.push(candle);
    }
    result
}

pub fn aggregate_trades_chain<'a>(trades: impl Iterator<Item = &'a Trade> + Clone, timeframes: &[Timeframe]) -> HashMap<Timeframe, Vec<Candle>> {
    let mut result = HashMap::new();
    if timeframes.is_empty() { return result; }
    let generator = CandleGenerator::default();
    let base_tf = timeframes[0].clone();
    let base_candles = generator.aggregate(trades.clone(), base_tf.clone());
    result.insert(base_tf.clone(), base_candles.clone());
    let mut prev = base_candles;
    for tf in timeframes.iter().skip(1) {
        let higher = aggregate_from_lower(&prev, tf);
        result.insert(tf.clone(), higher.clone());
        prev = higher;
    }
    result
}

#[derive(Debug, Serialize)]
pub struct SimpleCandle {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl From<&Candle> for SimpleCandle {
    fn from(c: &Candle) -> Self {
        Self {
            timestamp: c.timestamp.timestamp_millis(),
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: c.volume,
        }
    }
}

pub fn write_candles_csv<P: AsRef<Path>>(candles: &[Candle], out_path: P) -> Result<()> {
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(out_path)?;
    for candle in candles {
        wtr.serialize(SimpleCandle::from(candle))?;
    }
    wtr.flush()?;
    Ok(())
} 