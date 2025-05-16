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

#[cfg(test)]
mod tests {
    use super::*;
    use candle_generator::{Candle, Instrument, Pair, MarketType, Timeframe};
    use chrono::{Utc};

    fn sample_candle(ts: i64, open: f64) -> Candle {
        Candle {
            instrument: Instrument {
                pair: Pair { base_id: "BTC".to_string(), quote_id: "USDT".to_string() },
                exchange: "binance".to_string(),
                market_type: MarketType::Spot,
            },
            interval: Timeframe::m1,
            timestamp: Utc.timestamp_millis_opt(ts).unwrap(),
            open,
            high: open,
            low: open,
            close: open,
            volume: 1.0,
            trade_count: 1,
            volume_usdt: Some(open),
            custom: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_aggregate_chain_empty() {
        let candles: Vec<Candle> = vec![];
        let tfs = vec![Timeframe::m1];
        let result = aggregate_chain(&candles, &tfs).unwrap();
        assert!(result[&Timeframe::m1].is_empty());
    }

    #[test]
    fn test_aggregate_chain_one_candle() {
        let candles = vec![sample_candle(1714000000000, 50000.0)];
        let tfs = vec![Timeframe::m1];
        let result = aggregate_chain(&candles, &tfs).unwrap();
        assert_eq!(result[&Timeframe::m1].len(), 1);
    }

    #[test]
    fn test_aggregate_chain_multiple_candles() {
        let candles = vec![
            sample_candle(1714000000000, 50000.0),
            sample_candle(1714000060000, 50100.0),
            sample_candle(1714000120000, 50200.0),
        ];
        let tfs = vec![Timeframe::m1];
        let result = aggregate_chain(&candles, &tfs).unwrap();
        assert_eq!(result[&Timeframe::m1].len(), 3);
    }

    #[test]
    fn test_aggregate_chain_multiple_timeframes() {
        let candles = vec![
            sample_candle(1714000000000, 50000.0),
            sample_candle(1714000060000, 50100.0),
            sample_candle(1714000120000, 50200.0),
            sample_candle(1714000180000, 50300.0),
            sample_candle(1714000240000, 50400.0),
        ];
        let tfs = vec![Timeframe::m1, Timeframe::m5];
        let result = aggregate_chain(&candles, &tfs).unwrap();
        assert!(result[&Timeframe::m1].len() > 0);
        assert!(result[&Timeframe::m5].len() > 0);
    }
} 