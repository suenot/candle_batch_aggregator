use std::fs;
use std::path::PathBuf;
use reqwest::blocking::Client;
use candle_batch_aggregator::formats::clickhouse::process_clickhouse_batch;
use candle_batch_aggregator::Args;

fn main() {
    // 1. Генерируем тестовые трейды
    let trades = vec![
        (1714000000000i64, "binance", "BTC", "USDT", "Spot", "t1", 50000.0, 0.1, "Buy"),
        (1714000060000i64, "binance", "BTC", "USDT", "Spot", "t2", 50100.0, 0.2, "Sell"),
        (1714000120000i64, "binance", "BTC", "USDT", "Spot", "t3", 50200.0, 0.3, "Buy"),
    ];
    let input_dir = PathBuf::from("clickhouse_test_data/BTCUSDT");
    fs::create_dir_all(&input_dir).unwrap();

    // 2. Сохраняем трейды в ClickHouse через HTTP API
    let clickhouse_url = std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let table = "trades";
    let client = Client::new();
    // Создаём таблицу, если не существует
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            timestamp Int64,
            exchange String,
            base_id String,
            quote_id String,
            market_type String,
            id String,
            price Float64,
            amount Float64,
            side String
        ) ENGINE = MergeTree() ORDER BY timestamp",
        table
    );
    let _ = client.post(&format!("{}/?query={}", clickhouse_url, urlencoding::encode(&create_sql)))
        .body("")
        .send();
    // Вставляем трейды
    let mut values = String::new();
    for t in &trades {
        values.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            t.0, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8
        ));
    }
    let insert_sql = format!("INSERT INTO {} FORMAT TSV", table);
    let _ = client.post(&format!("{}/?query={}", clickhouse_url, urlencoding::encode(&insert_sql)))
        .body(values)
        .send();
    println!("Тестовые трейды сохранены в ClickHouse ({})", clickhouse_url);

    // 3. Создаём clickhouse_url.txt для batch-агрегации
    let url_path = input_dir.join("clickhouse_url.txt");
    if !url_path.exists() {
        let query = format!("SELECT * FROM trades ORDER BY timestamp FORMAT CSV");
        let url = format!("{}/?query={}", clickhouse_url, urlencoding::encode(&query));
        fs::write(&url_path, &url).unwrap();
        println!("Создан файл {:?} — укажите в нём URL ClickHouse, если нужно", url_path);
    }

    // 4. Запускаем batch-агрегацию
    let args = Args {
        input: PathBuf::from("clickhouse_test_data"),
        output: Some(PathBuf::from("clickhouse_test_out")),
        symbol: "BTCUSDT".to_string(),
        interval: "1,5".to_string(),
        format: "clickhouse".to_string(),
        benchmark: false,
        progress: false,
        memory_stats: false,
    };
    process_clickhouse_batch(&args).unwrap();

    // 5. Читаем результат
    let out_path = PathBuf::from("clickhouse_test_out/BTCUSDT_m1/clickhouse_m1.csv");
    if out_path.exists() {
        let content = std::fs::read_to_string(&out_path).unwrap();
        println!("\nАгрегированные свечи (m1):\n{}", content);
    } else {
        println!("Файл {:?} не найден", out_path);
    }
} 