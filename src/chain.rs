use anyhow::Result;
use candle_generator::{Candle, Timeframe, CandleGenerator};
use std::collections::HashMap;

pub fn aggregate_chain<'a>(candles: &'a [Candle], timeframes: &[Timeframe]) -> Result<HashMap<Timeframe, Vec<Candle>>> {
    let mut result = HashMap::new();
    if timeframes.is_empty() { return Ok(result); }
    // Первый таймфрейм — младший, уже есть свечи
    result.insert(timeframes[0].clone(), candles.to_vec());
    let mut prev = candles.to_vec();
    for tf in timeframes.iter().skip(1) {
        let generator = CandleGenerator::default();
        let higher = generator.aggregate(prev.iter(), tf.clone());
        result.insert(tf.clone(), higher.clone());
        prev = higher;
    }
    Ok(result)
} 