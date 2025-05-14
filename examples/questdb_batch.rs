use std::fs;
use std::path::PathBuf;
use candle_batch_aggregator::formats::questdb::process_questdb_batch;
use candle_batch_aggregator::Args;

fn main() {
    // 1. Подготовка: в questdb_test_data/BTCUSDT/questdb_url.txt должен быть URL на QuestDB HTTP API (CSV)
    // Например: http://localhost:9000/exec?query=SELECT+*+FROM+trades+ORDER+BY+timestamp&fmt=csv
    let input_dir = PathBuf::from("questdb_test_data/BTCUSDT");
    fs::create_dir_all(&input_dir).unwrap();
    let url_path = input_dir.join("questdb_url.txt");
    if !url_path.exists() {
        fs::write(&url_path, "http://localhost:9000/exec?query=SELECT+*+FROM+trades+ORDER+BY+timestamp&fmt=csv").unwrap();
        println!("Создан файл {:?} — укажите в нём URL QuestDB", url_path);
        return;
    }

    // 2. Запускаем batch-агрегацию
    let args = Args {
        input: PathBuf::from("questdb_test_data"),
        output: Some(PathBuf::from("questdb_test_out")),
        symbol: "BTCUSDT".to_string(),
        interval: "1,5".to_string(),
        format: "questdb".to_string(),
        benchmark: false,
        progress: false,
        memory_stats: false,
    };
    candle_batch_aggregator::formats::questdb::process_questdb_batch(&args).unwrap();

    // 3. Читаем результат
    let out_path = PathBuf::from("questdb_test_out/BTCUSDT_m1/questdb_m1.csv");
    if out_path.exists() {
        let content = std::fs::read_to_string(&out_path).unwrap();
        println!("\nАгрегированные свечи (m1):\n{}", content);
    } else {
        println!("Файл {:?} не найден", out_path);
    }
} 