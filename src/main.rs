use clap::Parser;
use log::{error, info};
use serde::{Deserialize, Serialize};
use solana_ledger::shred::{Shred, ShredId};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long)]
    pub name_0: String,
    #[clap(long)]
    pub port_0: u16,
    #[clap(short, long)]
    pub name_1: String,
    #[clap(short, long)]
    pub port_1: u16,
    #[clap(long, default_value = "120")]
    pub timeout_secs: u64,
    #[clap(long)]
    pub output_file: Option<String>,
}

#[derive(Debug)]
enum ProcessorEvent {
    ShredReceived {
        port_id: u8,
        name: Arc<str>,
        shred_id: ShredId,
        timestamp: Instant,
    },
    Cleanup,
    StatsTick,
    SaveStats,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Percentiles {
    p50: f64,
    p90: f64,
    p99: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EndpointSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    first_shred_delay: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    processing_delay: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    confirmation_delay: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finalization_delay: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    download_time: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replay_time: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    confirmation_time: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finalization_time: Option<Percentiles>,
    account_delay: Option<Percentiles>, // 总是序列化，即使为 None（会序列化为 null）
    // 添加我们自己的统计字段
    #[serde(skip_serializing_if = "Option::is_none")]
    lead_time: Option<Percentiles>,
    #[serde(skip_serializing_if = "Option::is_none")]
    diff_time: Option<Percentiles>,
}

#[derive(Serialize, Deserialize, Debug)]
struct StatsData {
    endpoint1_summary: EndpointSummary,
    endpoint2_summary: EndpointSummary,
    slots: Vec<serde_json::Value>,
}

struct ProcessorState {
    port0_data: HashMap<ShredId, Instant>,
    port1_data: HashMap<ShredId, Instant>,
    matched_pairs: usize,
    // First seen 统计
    first_seen_port0: usize, // port0 先看到的 shred 数量
    first_seen_port1: usize, // port1 先看到的 shred 数量
    // Lead time: 当 port0 先到达时的延迟（port1_time - port0_time，正数，单位：纳秒）
    lead_times_ns: Vec<i64>,
    // Diff time: 所有匹配的延迟（port0_time - port1_time，可能是负数，单位：纳秒）
    all_diffs_ns: Vec<i64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 默认设置为 info 级别，无需设置 RUST_LOG 环境变量
    pretty_env_logger::formatted_timed_builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let args = Args::parse();

    let (processor_tx, mut processor_rx) = mpsc::channel(4096);

    let port0_task = start_port_listener(
        0,
        args.name_0.clone().into(),
        args.port_0,
        processor_tx.clone(),
    );
    let port1_task = start_port_listener(
        1,
        args.name_1.clone().into(),
        args.port_1,
        processor_tx.clone(),
    );

    let timer_task = {
        let processor_tx = processor_tx.clone();
        tokio::spawn(async move {
            let mut cleanup_interval = time::interval(Duration::from_secs(args.timeout_secs));
            let mut stats_interval = time::interval(Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = cleanup_interval.tick() => {
                        processor_tx.send(ProcessorEvent::Cleanup).await.ok();
                    }
                    _ = stats_interval.tick() => {
                        processor_tx.send(ProcessorEvent::StatsTick).await.ok();
                    }
                }
            }
        })
    };

    let output_file = args.output_file.clone();
    let processor_tx_for_save = processor_tx.clone();
    let processor_task = tokio::spawn(async move {
        let mut state = ProcessorState {
            port0_data: HashMap::new(),
            port1_data: HashMap::new(),
            matched_pairs: 0,
            first_seen_port0: 0,
            first_seen_port1: 0,
            lead_times_ns: Vec::new(),
            all_diffs_ns: Vec::new(),
        };

        while let Some(event) = processor_rx.recv().await {
            match event {
                ProcessorEvent::ShredReceived {
                    port_id,
                    name,
                    shred_id,
                    timestamp,
                } => {
                    process_shred(&mut state, port_id, name, shred_id, timestamp);
                }
                ProcessorEvent::Cleanup => {
                    cleanup_data(&mut state, Duration::from_secs(args.timeout_secs));
                }
                ProcessorEvent::StatsTick => {
                    report_stats(&state, &args);
                }
                ProcessorEvent::SaveStats => {
                    if let Some(output_file) = &output_file {
                        if let Err(e) = save_stats_to_json(&state, &args, output_file) {
                            error!("Failed to save stats to JSON: {}", e);
                        } else {
                            info!("Statistics saved to {}", output_file);
                        }
                    }
                }
            }
        }

        // 在退出前保存统计数据
        if let Some(output_file) = &output_file {
            if let Err(e) = save_stats_to_json(&state, &args, output_file) {
                error!("Failed to save stats to JSON: {}", e);
            } else {
                info!("Statistics saved to {}", output_file);
            }
        }
    });

    tokio::select! {
        _ = port0_task => {},
        _ = port1_task => {},
        _ = processor_task => {},
        _ = timer_task => {},
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down...");
            // 发送保存统计数据的请求
            processor_tx_for_save.send(ProcessorEvent::SaveStats).await.ok();
            // 等待一小段时间确保保存完成
            tokio::time::sleep(Duration::from_millis(100)).await;
        },
    }

    Ok(())
}

fn start_port_listener(
    port_id: u8,
    name: Arc<str>,
    port: u16,
    sender: mpsc::Sender<ProcessorEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let socket = match UdpSocket::bind(format!("0.0.0.0:{}", port)).await {
            Ok(s) => s,
            Err(e) => {
                error!("[{}] Failed to bind port {}: {}", name, port, e);
                return;
            }
        };
        info!("[{}] Listening on port {}", name, port);

        let mut buf = [0u8; 2048];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((size, _)) => {
                    let data = buf[..size].to_vec();
                    if let Ok(shred) = Shred::new_from_serialized_shred(data) {
                        let event = ProcessorEvent::ShredReceived {
                            port_id,
                            name: Arc::clone(&name),
                            shred_id: shred.id(),
                            timestamp: Instant::now(),
                        };
                        if let Err(e) = sender.send(event).await {
                            error!("[{}] Failed to send event: {}", name, e);
                        }
                    }
                }
                Err(e) => error!("[{}] Receive error: {}", name, e),
            }
        }
    })
}

fn process_shred(
    state: &mut ProcessorState,
    port_id: u8,
    name: Arc<str>,
    shred_id: ShredId,
    timestamp: Instant,
) {
    match port_id {
        0 => {
            if state.port0_data.contains_key(&shred_id) {
                return;
            }
            let is_first_seen = !state.port1_data.contains_key(&shred_id);
            if is_first_seen {
                state.first_seen_port0 += 1;
            }
            state.port0_data.insert(shred_id.clone(), timestamp);
            if let Some(port1_time) = state.port1_data.get(&shred_id) {
                state.matched_pairs += 1;
                // all_diffs: port0_time - port1_time (纳秒，可以是负数)
                let diff_ns = if timestamp >= *port1_time {
                    timestamp.duration_since(*port1_time).as_nanos() as i64
                } else {
                    -(port1_time.duration_since(timestamp).as_nanos() as i64)
                };
                state.all_diffs_ns.push(diff_ns);

                // lead_times: 当 port0 先到达时，port0 领先的时间 = port1_time - port0_time（正数）
                if is_first_seen && timestamp < *port1_time {
                    let lead_time_ns = port1_time.duration_since(timestamp).as_nanos() as i64;
                    state.lead_times_ns.push(lead_time_ns);
                }
            }
        }
        1 => {
            if state.port1_data.contains_key(&shred_id) {
                return;
            }
            let is_first_seen = !state.port0_data.contains_key(&shred_id);
            if is_first_seen {
                state.first_seen_port1 += 1;
            }
            state.port1_data.insert(shred_id.clone(), timestamp);
            if let Some(port0_time) = state.port0_data.get(&shred_id) {
                state.matched_pairs += 1;
                // all_diffs: port0_time - port1_time (纳秒，可以是负数)
                let diff_ns = if *port0_time >= timestamp {
                    port0_time.duration_since(timestamp).as_nanos() as i64
                } else {
                    -(timestamp.duration_since(*port0_time).as_nanos() as i64)
                };
                state.all_diffs_ns.push(diff_ns);

                // lead_times: 当 port0 先到达时，port0 领先的时间 = port1_time - port0_time（正数）
                if !is_first_seen && *port0_time < timestamp {
                    let lead_time_ns = timestamp.duration_since(*port0_time).as_nanos() as i64;
                    state.lead_times_ns.push(lead_time_ns);
                }
            }
        }
        _ => unreachable!(),
    }
}

fn cleanup_data(state: &mut ProcessorState, timeout: Duration) {
    let now = Instant::now();
    state
        .port0_data
        .retain(|_, t| now.duration_since(*t) < timeout);
    state
        .port1_data
        .retain(|_, t| now.duration_since(*t) < timeout);
    info!("Cleanup completed");
}

fn report_stats(state: &ProcessorState, args: &Args) {
    let total_first_seen = state.first_seen_port0 + state.first_seen_port1;
    let port0_percent = if total_first_seen > 0 {
        (state.first_seen_port0 as f64 / total_first_seen as f64) * 100.0
    } else {
        0.0
    };
    let port1_percent = if total_first_seen > 0 {
        (state.first_seen_port1 as f64 / total_first_seen as f64) * 100.0
    } else {
        0.0
    };

    info!("First seen shred in 1min");
    info!("");
    info!("{{From:{}, Nums:{}, Percent:{:.1}%}}, {{From:others, Nums:0, Percent:0.0%}}, {{From:{}, Nums:{}, Percent:{:.1}%}}",
        args.name_0, state.first_seen_port0, port0_percent,
        args.name_1, state.first_seen_port1, port1_percent);

    // Target-led shred lead time
    if !state.lead_times_ns.is_empty() {
        let percentiles = calculate_percentiles_i64(&state.lead_times_ns);
        info!(
            "{}-led shred lead time (n={}) against {}: P1={}, P5={}, P10={}, P25={}, P50={}, P75={}, P80={}, P90={}, P95={}, P99={}",
            args.name_0,
            state.lead_times_ns.len(),
            args.name_1,
            format_nanos(percentiles[0]),
            format_nanos(percentiles[1]),
            format_nanos(percentiles[2]),
            format_nanos(percentiles[3]),
            format_nanos(percentiles[4]),
            format_nanos(percentiles[5]),
            format_nanos(percentiles[6]),
            format_nanos(percentiles[7]),
            format_nanos(percentiles[8]),
            format_nanos(percentiles[9]),
        );
    }

    // Target-diff shred diff time
    if !state.all_diffs_ns.is_empty() {
        let percentiles = calculate_percentiles_i64(&state.all_diffs_ns);
        info!(
            "{}-diff shred diff time (n={}) against {}: P1={}, P5={}, P10={}, P25={}, P50={}, P75={}, P80={}, P90={}, P95={}, P99={}",
            args.name_0,
            state.all_diffs_ns.len(),
            args.name_1,
            format_nanos(percentiles[0]),
            format_nanos(percentiles[1]),
            format_nanos(percentiles[2]),
            format_nanos(percentiles[3]),
            format_nanos(percentiles[4]),
            format_nanos(percentiles[5]),
            format_nanos(percentiles[6]),
            format_nanos(percentiles[7]),
            format_nanos(percentiles[8]),
            format_nanos(percentiles[9]),
        );
    }
}

fn calculate_percentiles_i64(data: &[i64]) -> [i64; 10] {
    if data.is_empty() {
        return [0; 10];
    }

    let mut sorted = data.to_vec();
    sorted.sort();

    let len = sorted.len();
    let percentiles = [1, 5, 10, 25, 50, 75, 80, 90, 95, 99];

    let mut result = [0; 10];
    for (i, &p) in percentiles.iter().enumerate() {
        let index = ((len - 1) as f64 * p as f64 / 100.0).round() as usize;
        result[i] = sorted[index.min(len - 1)];
    }

    result
}

fn format_nanos(nanos: i64) -> String {
    let abs_nanos = nanos.abs() as f64;
    let sign = if nanos < 0 { "-" } else { "" };

    if abs_nanos >= 1_000_000.0 {
        format!("{}{:.6}ms", sign, abs_nanos / 1_000_000.0)
    } else if abs_nanos >= 1_000.0 {
        format!("{}{:.3}µs", sign, abs_nanos / 1_000.0)
    } else {
        format!("{}{}ns", sign, nanos)
    }
}

fn calculate_percentiles_f64(data: &[i64]) -> Option<Percentiles> {
    if data.is_empty() {
        return None;
    }

    let mut sorted: Vec<f64> = data.iter().map(|&x| x as f64 / 1_000_000.0).collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let len = sorted.len();
    let p50_idx = ((len - 1) as f64 * 0.5).round() as usize;
    let p90_idx = ((len - 1) as f64 * 0.9).round() as usize;
    let p99_idx = ((len - 1) as f64 * 0.99).round() as usize;

    Some(Percentiles {
        p50: sorted[p50_idx.min(len - 1)],
        p90: sorted[p90_idx.min(len - 1)],
        p99: sorted[p99_idx.min(len - 1)],
    })
}

fn save_stats_to_json(
    state: &ProcessorState,
    args: &Args,
    output_file: &str,
) -> anyhow::Result<()> {
    let endpoint1_summary = EndpointSummary {
        first_shred_delay: None,
        processing_delay: None,
        confirmation_delay: None,
        finalization_delay: None,
        download_time: None,
        replay_time: None,
        confirmation_time: None,
        finalization_time: None,
        account_delay: None,
        lead_time: calculate_percentiles_f64(&state.lead_times_ns),
        diff_time: calculate_percentiles_f64(&state.all_diffs_ns),
    };

    let endpoint2_summary = EndpointSummary {
        first_shred_delay: None,
        processing_delay: None,
        confirmation_delay: None,
        finalization_delay: None,
        download_time: None,
        replay_time: None,
        confirmation_time: None,
        finalization_time: None,
        account_delay: None,
        lead_time: None,
        diff_time: None,
    };

    let stats_data = StatsData {
        endpoint1_summary,
        endpoint2_summary,
        slots: Vec::new(),
    };

    let json = serde_json::to_string_pretty(&stats_data)?;
    fs::write(output_file, json)?;

    Ok(())
}
