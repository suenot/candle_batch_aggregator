use anyhow::Result;
use candle_generator::{Candle, Timeframe};
use std::collections::HashMap;

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
            timestamp: slice[0].timestamp,
            open, high, low, close, volume, trade_count, volume_usdt,
            custom: std::collections::HashMap::new(),
        };
        i += count;
        result.push(candle);
    }
    result
}

pub fn aggregate_chain<'a>(candles: &'a [Candle], timeframes: &[Timeframe]) -> Result<HashMap<Timeframe, Vec<Candle>>> {
    let mut result = HashMap::new();
    if timeframes.is_empty() { return Ok(result); }
    // Первый таймфрейм — младший, уже есть свечи
    result.insert(timeframes[0].clone(), candles.to_vec());
    let mut prev = candles.to_vec();
    for tf in timeframes.iter().skip(1) {
        let higher = aggregate_from_lower(&prev, tf);
        result.insert(tf.clone(), higher.clone());
        prev = higher;
    }
    Ok(result)
} 