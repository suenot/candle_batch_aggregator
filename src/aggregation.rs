use anyhow::Result;
use candle_generator::{Candle, Timeframe, CandleGenerator, Trade};
use std::collections::HashMap;
use std::path::Path;
use csv::WriterBuilder;
use serde::Serialize;

pub fn aggregate_trades_to_candles(/* trades, interval, ... */) -> Result<()> {
    // TODO: реализовать агрегацию через candle_generator
    Ok(())
}

pub fn aggregate_trades_chain<'a>(trades: impl Iterator<Item = &'a Trade>, timeframes: &[Timeframe]) -> HashMap<Timeframe, Vec<Candle>> {
    let mut result = HashMap::new();
    if timeframes.is_empty() { return result; }
    let generator = CandleGenerator::default();
    let base_tf = timeframes[0].clone();
    let base_candles = generator.aggregate(trades, base_tf.clone());
    result.insert(base_tf.clone(), base_candles.clone());
    let mut prev = base_candles;
    for tf in timeframes.iter().skip(1) {
        let higher = generator.aggregate(prev.iter(), tf.clone());
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