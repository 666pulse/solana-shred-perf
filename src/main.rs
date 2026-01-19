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
    #[clap(long, default_value = "stats.json")]
    pub output_file: String,
    #[clap(long, default_value_t = 1000)]
    pub max_slots: u64,
}

#[derive(Debug)]
enum ProcessorEvent {
    ShredReceived {
        port_id: u8,
        name: Arc<str>,
        shred_id: ShredId,
        slot: u64,
        timestamp: Instant,
    },
    Cleanup,
    StatsTick,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SlotEndpointData {
    first_shred_delay_ms: f64,
    processing_delay_ms: f64,
    account_updates: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SlotData {
    slot: u64,
    endpoint1: SlotEndpointData,
    endpoint2: SlotEndpointData,
}

#[derive(Serialize, Deserialize, Debug)]
struct StatsData {
    endpoint1_summary: EndpointSummary,
    endpoint2_summary: EndpointSummary,
    slots: Vec<SlotData>,
}

struct SlotInfo {
    port0_first_shred_time: Option<Instant>,
    port1_first_shred_time: Option<Instant>,
    port0_shreds: Vec<(ShredId, Instant)>,
    port1_shreds: Vec<(ShredId, Instant)>,
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
    // Slot 统计
    seen_slots: std::collections::HashSet<u64>,
    // Slot 详细信息
    slot_data: std::collections::HashMap<u64, SlotInfo>,
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
    let max_slots = args.max_slots;
    let processor_tx_clone = processor_tx.clone();
    let processor_task = tokio::spawn(async move {
        let mut state = ProcessorState {
            port0_data: HashMap::new(),
            port1_data: HashMap::new(),
            matched_pairs: 0,
            first_seen_port0: 0,
            first_seen_port1: 0,
            lead_times_ns: Vec::new(),
            all_diffs_ns: Vec::new(),
            seen_slots: std::collections::HashSet::new(),
            slot_data: std::collections::HashMap::new(),
        };

        while let Some(event) = processor_rx.recv().await {
            match event {
                ProcessorEvent::ShredReceived {
                    port_id,
                    name,
                    shred_id,
                    slot,
                    timestamp,
                } => {
                    let new_slot = state.seen_slots.insert(slot);
                    if new_slot {
                        if max_slots > 0 && state.seen_slots.len() >= max_slots as usize {
                            info!(
                                "Reached max slots limit ({}), saving statistics...",
                                max_slots
                            );
                            // 保存数据
                            if let Err(e) = save_stats_to_json(&state, &args, &output_file) {
                                error!("Failed to save stats to JSON: {}", e);
                            } else {
                                info!("Statistics saved to {}", output_file);
                            }
                            // 关闭 channel，让监听器自然退出
                            drop(processor_rx);
                            drop(processor_tx_clone);
                            // 退出循环，processor_task 完成
                            break;
                        }
                    }
                    process_shred(&mut state, port_id, name, shred_id, slot, timestamp);
                }
                ProcessorEvent::Cleanup => {
                    cleanup_data(&mut state, Duration::from_secs(args.timeout_secs));
                }
                ProcessorEvent::StatsTick => {
                    if max_slots > 0 {
                        info!("Processed {} / {} slots", state.seen_slots.len(), max_slots);
                    }
                    report_stats(&state, &args);
                }
            }
        }
    });

    tokio::select! {
        _ = port0_task => {},
        _ = port1_task => {},
        result = processor_task => {
            if let Err(e) = result {
                error!("Processor task error: {:?}", e);
            } else {
                info!("Program completed successfully");
            }
        },
        _ = timer_task => {},
        _ = tokio::signal::ctrl_c() => {
            info!("Interrupted by user, exiting...");
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
        let mut has_printed_version = false;
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((size, _)) => {
                    let data = buf[..size].to_vec();
                    if let Ok(shred) = Shred::new_from_serialized_shred(data) {
                        let shred_id = shred.id();
                        // 从 shred 中获取 slot
                        let slot = shred.slot();

                        // 只在第一次收到 shred 时打印 version（用于确认连接）
                        if !has_printed_version {
                            let version = shred.version();
                            info!("[{}] Shred version: {}, slot: {}, index: {}", name, version, slot, shred_id.index());
                            has_printed_version = true;
                        }
                        let event = ProcessorEvent::ShredReceived {
                            port_id,
                            name: Arc::clone(&name),
                            shred_id,
                            slot,
                            timestamp: Instant::now(),
                        };
                        if let Err(_) = sender.send(event).await {
                            // Channel 已关闭，正常退出
                            break;
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
    slot: u64,
    timestamp: Instant,
) {
    // 更新 slot 信息
    let slot_info = state.slot_data.entry(slot).or_insert_with(|| SlotInfo {
        port0_first_shred_time: None,
        port1_first_shred_time: None,
        port0_shreds: Vec::new(),
        port1_shreds: Vec::new(),
    });

    match port_id {
        0 => {
            if slot_info.port0_first_shred_time.is_none() {
                slot_info.port0_first_shred_time = Some(timestamp);
            }
            slot_info.port0_shreds.push((shred_id.clone(), timestamp));
        }
        1 => {
            if slot_info.port1_first_shred_time.is_none() {
                slot_info.port1_first_shred_time = Some(timestamp);
            }
            slot_info.port1_shreds.push((shred_id.clone(), timestamp));
        }
        _ => {}
    }
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

    // 构建 slots 数据
    let mut slots = Vec::new();
    let mut sorted_slots: Vec<_> = state.slot_data.iter().collect();
    sorted_slots.sort_by_key(|(slot, _)| **slot);

    for (slot, slot_info) in sorted_slots {
        // 计算延迟（相对于第一个端点的延迟）
        let first_time = slot_info
            .port0_first_shred_time
            .or(slot_info.port1_first_shred_time)
            .unwrap_or_else(|| Instant::now());

        // first_shred_delay: 如果这个端点先收到，延迟为 0；否则是相对于另一个端点的延迟
        let endpoint1_first_shred_delay = match (
            slot_info.port0_first_shred_time,
            slot_info.port1_first_shred_time,
        ) {
            (Some(t0), Some(t1)) => {
                if t0 <= t1 {
                    0.0 // endpoint1 先收到
                } else {
                    t0.duration_since(t1).as_secs_f64() * 1000.0 // endpoint1 延迟
                }
            }
            (Some(_), None) => 0.0, // 只有 endpoint1 收到
            (None, Some(t1)) => {
                // endpoint1 没收到，但 endpoint2 收到了，延迟很大
                Instant::now().duration_since(t1).as_secs_f64() * 1000.0
            }
            (None, None) => 0.0,
        };

        let endpoint2_first_shred_delay = match (
            slot_info.port0_first_shred_time,
            slot_info.port1_first_shred_time,
        ) {
            (Some(t0), Some(t1)) => {
                if t1 <= t0 {
                    0.0 // endpoint2 先收到
                } else {
                    t1.duration_since(t0).as_secs_f64() * 1000.0 // endpoint2 延迟
                }
            }
            (None, Some(_)) => 0.0, // 只有 endpoint2 收到
            (Some(t0), None) => {
                // endpoint2 没收到，但 endpoint1 收到了，延迟很大
                Instant::now().duration_since(t0).as_secs_f64() * 1000.0
            }
            (None, None) => 0.0,
        };

        // 计算 processing_delay（这里简化处理，使用第一个和最后一个 shred 的时间差）
        let endpoint1_processing_delay = if let (Some(t0), Some(t1)) = (
            slot_info.port0_first_shred_time,
            slot_info.port0_shreds.last().map(|(_, t)| *t),
        ) {
            if let Some(last) = slot_info.port0_shreds.last() {
                last.1.duration_since(t0).as_secs_f64() * 1000.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        let endpoint2_processing_delay = if let (Some(t1), _) = (
            slot_info.port1_first_shred_time,
            slot_info.port1_shreds.last().map(|(_, t)| *t),
        ) {
            if let Some(last) = slot_info.port1_shreds.last() {
                last.1.duration_since(t1).as_secs_f64() * 1000.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        let slot_data = SlotData {
            slot: *slot,
            endpoint1: SlotEndpointData {
                first_shred_delay_ms: endpoint1_first_shred_delay,
                processing_delay_ms: endpoint1_processing_delay,
                account_updates: Vec::new(),
            },
            endpoint2: SlotEndpointData {
                first_shred_delay_ms: endpoint2_first_shred_delay,
                processing_delay_ms: endpoint2_processing_delay,
                account_updates: Vec::new(),
            },
        };
        slots.push(slot_data);
    }

    let stats_data = StatsData {
        endpoint1_summary,
        endpoint2_summary,
        slots,
    };

    let json = serde_json::to_string_pretty(&stats_data)?;
    fs::write(output_file, json)?;

    Ok(())
}
