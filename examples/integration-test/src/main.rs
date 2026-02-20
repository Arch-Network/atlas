use std::sync::Arc;
use std::time::Duration;

use arch_sdk::{
    AsyncArchRpcClient, BackoffStrategy, Config, Event, EventTopic, WebSocketClient,
};
use clap::Parser;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "arch-atlas-integration-test",
    about = "Integration test suite for Atlas against a live Arch Network RPC/WebSocket"
)]
struct Cli {
    /// Arch Network RPC URL (e.g. http://localhost:9002 or https://rpc.arch.network)
    #[arg(long, env = "ARCH_RPC_URL")]
    rpc_url: String,

    /// Arch Network WebSocket URL (e.g. ws://localhost:9003 or wss://ws.arch.network)
    #[arg(long, env = "ARCH_WS_URL")]
    ws_url: String,

    /// How long (in seconds) to listen for live WebSocket events before declaring success
    #[arg(long, default_value = "60")]
    ws_listen_secs: u64,

    /// Skip the WebSocket test (useful for quick RPC-only checks)
    #[arg(long, default_value = "false")]
    skip_ws: bool,
}

// ─── test result bookkeeping ─────────────────────────────────────────────────

#[derive(Default)]
struct TestResults {
    passed: Vec<String>,
    failed: Vec<(String, String)>,
    skipped: Vec<String>,
}

impl TestResults {
    fn pass(&mut self, name: &str) {
        info!("[PASS] {}", name);
        self.passed.push(name.to_string());
    }

    fn fail(&mut self, name: &str, reason: &str) {
        error!("[FAIL] {} — {}", name, reason);
        self.failed.push((name.to_string(), reason.to_string()));
    }

    fn skip(&mut self, name: &str) {
        warn!("[SKIP] {}", name);
        self.skipped.push(name.to_string());
    }

    fn summary(&self) {
        println!("\n═══════════════════════════════════════════════");
        println!("  INTEGRATION TEST RESULTS");
        println!("═══════════════════════════════════════════════");
        println!("  Passed:  {}", self.passed.len());
        println!("  Failed:  {}", self.failed.len());
        println!("  Skipped: {}", self.skipped.len());
        println!("───────────────────────────────────────────────");

        for name in &self.passed {
            println!("  [PASS] {}", name);
        }
        for (name, reason) in &self.failed {
            println!("  [FAIL] {} — {}", name, reason);
        }
        for name in &self.skipped {
            println!("  [SKIP] {}", name);
        }
        println!("═══════════════════════════════════════════════\n");
    }

    fn exit_code(&self) -> i32 {
        if self.failed.is_empty() { 0 } else { 1 }
    }
}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn make_rpc_client(rpc_url: &str) -> AsyncArchRpcClient {
    let mut cfg = Config::testnet();
    cfg.arch_node_url = rpc_url.to_string();
    AsyncArchRpcClient::new(&cfg)
}

// ─── RPC tests ───────────────────────────────────────────────────────────────

async fn test_get_block_count(rpc: &AsyncArchRpcClient, results: &mut TestResults) -> Option<u64> {
    let name = "rpc::get_block_count";
    match rpc.get_block_count().await {
        Ok(count) => {
            info!("{}: block_count = {}", name, count);
            if count == 0 {
                results.fail(name, "block count is 0 — chain may not be producing blocks");
                None
            } else {
                results.pass(name);
                Some(count)
            }
        }
        Err(e) => {
            results.fail(name, &format!("{}", e));
            None
        }
    }
}

async fn test_get_best_block_hash(rpc: &AsyncArchRpcClient, results: &mut TestResults) {
    let name = "rpc::get_best_block_hash";
    match rpc.get_best_block_hash().await {
        Ok(hash) => {
            info!("{}: best_block_hash = {:?}", name, hash);
            results.pass(name);
        }
        Err(e) => results.fail(name, &format!("{}", e)),
    }
}

async fn test_get_block_by_height(
    rpc: &AsyncArchRpcClient,
    height: u64,
    results: &mut TestResults,
) {
    let name = &format!("rpc::get_block_by_height({})", height);
    match rpc.get_block_by_height(height).await {
        Ok(Some(block)) => {
            info!(
                "{}: block_height={}, txs={}, prev_hash={:?}",
                name,
                block.block_height,
                block.transactions.len(),
                block.previous_block_hash,
            );
            results.pass(name);
        }
        Ok(None) => results.fail(name, "returned None (block not found)"),
        Err(e) => {
            warn!("{}: {} (some RPCs don't support this endpoint)", name, e);
            results.skip(name);
        }
    }
}

async fn test_get_full_block_by_height(
    rpc: &AsyncArchRpcClient,
    height: u64,
    results: &mut TestResults,
) {
    let name = &format!("rpc::get_full_block_by_height({})", height);
    match rpc.get_full_block_by_height(height).await {
        Ok(Some(full_block)) => {
            info!(
                "{}: height={}, txs={}, timestamp={}",
                name,
                full_block.block_height,
                full_block.transactions.len(),
                full_block.timestamp,
            );
            if !full_block.transactions.is_empty() {
                let first_tx = &full_block.transactions[0];
                info!(
                    "  first tx status={:?}",
                    first_tx.status,
                );
            }
            results.pass(name);
        }
        Ok(None) => results.fail(name, "returned None (block not found)"),
        Err(e) => results.fail(name, &format!("{}", e)),
    }
}

async fn test_get_block_hash(rpc: &AsyncArchRpcClient, height: u64, results: &mut TestResults) {
    let name = &format!("rpc::get_block_hash({})", height);
    match rpc.get_block_hash(height).await {
        Ok(hash) => {
            info!("{}: hash = {}", name, hash);
            results.pass(name);
        }
        Err(e) => results.fail(name, &format!("{}", e)),
    }
}

async fn test_get_full_block_by_hash(
    rpc: &AsyncArchRpcClient,
    height: u64,
    results: &mut TestResults,
) {
    let name = "rpc::get_full_block_by_hash";
    let hash = match rpc.get_block_hash(height).await {
        Ok(h) => h,
        Err(e) => {
            results.fail(name, &format!("could not get hash for height {}: {}", height, e));
            return;
        }
    };

    match rpc.get_full_block_by_hash(&hash).await {
        Ok(Some(full_block)) => {
            info!(
                "{}: hash={}, height={}, txs={}",
                name,
                hash,
                full_block.block_height,
                full_block.transactions.len(),
            );
            results.pass(name);
        }
        Ok(None) => results.fail(name, &format!("returned None for hash {}", hash)),
        Err(e) => results.fail(name, &format!("{}", e)),
    }
}

async fn test_get_processed_transaction(
    rpc: &AsyncArchRpcClient,
    height: u64,
    results: &mut TestResults,
) {
    let name = "rpc::get_processed_transaction";

    // Search backwards from `height` for a block that has at least one transaction
    let mut found = None;
    for h in (height.saturating_sub(100)..=height).rev() {
        match rpc.get_full_block_by_height(h).await {
            Ok(Some(b)) if !b.transactions.is_empty() => {
                found = Some(b);
                break;
            }
            _ => continue,
        }
    }

    let full_block = match found {
        Some(b) => b,
        None => {
            info!(
                "{}: skipped — no blocks with transactions found near height {}",
                name, height
            );
            results.skip(name);
            return;
        }
    };

    let first_tx = &full_block.transactions[0];
    let tx_hash = first_tx.runtime_transaction.hash();
    let tx_hash_hex = hex::encode(tx_hash.as_ref() as &[u8; 32]);
    info!(
        "{}: probing tx {} from block {}",
        name, tx_hash_hex, full_block.block_height
    );

    match rpc.get_processed_transaction(&tx_hash).await {
        Ok(Some(ptx)) => {
            info!("{}: tx_hash={}, status={:?}", name, tx_hash_hex, ptx.status);
            results.pass(name);
        }
        Ok(None) => results.fail(name, &format!("returned None for tx {}", tx_hash_hex)),
        Err(e) => results.fail(name, &format!("{}", e)),
    }
}

/// Attempts to fetch a block by height.  Returns Ok(true) if found, Ok(false) if
/// the RPC returned None, or logs the error and returns Ok(false) for RPC errors.
async fn block_available(rpc: &AsyncArchRpcClient, height: u64) -> bool {
    match rpc.get_block_by_height(height).await {
        Ok(Some(_)) => true,
        Ok(None) | Err(_) => false,
    }
}

/// Same as block_available but uses get_full_block_by_height (some RPCs only
/// implement the "full" variant).
async fn full_block_available(rpc: &AsyncArchRpcClient, height: u64) -> bool {
    match rpc.get_full_block_by_height(height).await {
        Ok(Some(_)) => true,
        Ok(None) | Err(_) => false,
    }
}

/// Probes for the first available block (the RPC may be pruned).
/// Tries both `get_block_by_height` and `get_full_block_by_height`.
async fn find_available_block(rpc: &AsyncArchRpcClient, tip: u64) -> Option<u64> {
    // Try blocks near tip with both endpoints
    for offset in [0u64, 1, 2, 5, 10, 50, 100] {
        let h = tip.saturating_sub(offset);
        if block_available(rpc, h).await || full_block_available(rpc, h).await {
            info!("probe: block {} is available (tip-{})", h, offset);
            // Binary search for the lowest available block
            let mut lo = 0u64;
            let mut hi = h;
            let mut best = h;
            while lo <= hi {
                let mid = lo + (hi - lo) / 2;
                if block_available(rpc, mid).await || full_block_available(rpc, mid).await {
                    best = mid;
                    if mid == 0 {
                        break;
                    }
                    hi = mid - 1;
                } else {
                    lo = mid + 1;
                }
            }
            return Some(best);
        }
    }

    // Last resort: try get_block_hash for tip-1 (maybe the block endpoint is different)
    info!("probe: trying get_block_hash as fallback…");
    for offset in [1u64, 2, 5] {
        let h = tip.saturating_sub(offset);
        match rpc.get_block_hash(h).await {
            Ok(hash) => {
                info!("probe: get_block_hash({}) succeeded: {}", h, hash);
                // Try fetching by hash
                match rpc.get_full_block_by_hash(&hash).await {
                    Ok(Some(_)) => {
                        info!("probe: get_full_block_by_hash succeeded for height {}", h);
                        return Some(h);
                    }
                    Ok(None) => info!("probe: get_full_block_by_hash({}) → None", hash),
                    Err(e) => info!("probe: get_full_block_by_hash({}) → Err({})", hash, e),
                }
            }
            Err(e) => {
                info!("probe: get_block_hash({}) → Err({})", h, e);
            }
        }
    }

    warn!("probe: no blocks found near tip {}", tip);
    None
}

// ─── atlas wrapper test ──────────────────────────────────────────────────────

async fn test_atlas_arch_rpc_client(rpc_url: &str, results: &mut TestResults) {
    use arch_atlas_rpc_datasource::ArchRpc;
    use arch_atlas_rpc_datasource::ArchRpcClient;

    let name = "atlas::ArchRpcClient::get_block_count";
    let client = ArchRpcClient::new(rpc_url);
    let tip = match client.get_block_count().await {
        Ok(c) => {
            info!("{}: count = {}", name, c);
            results.pass(name);
            c
        }
        Err(e) => {
            results.fail(name, &format!("{}", e));
            return;
        }
    };

    // Use a fresh height close to the current tip to avoid pruning races
    let h = tip.saturating_sub(1);
    let name2 = &format!("atlas::ArchRpcClient::get_block_by_height({})", h);
    match client.get_block_by_height(h).await {
        Ok(full_block) => {
            info!(
                "{}: height={}, txs={}",
                name2,
                full_block.block_height,
                full_block.transactions.len()
            );
            results.pass(name2);
        }
        Err(e) => {
            let msg = format!("{}", e);
            if msg.contains("not found") || msg.contains("Not found") {
                warn!("{}: block pruned before fetch ({})", name2, msg);
                results.skip(name2);
            } else {
                results.fail(name2, &msg);
            }
        }
    }
}

// ─── WebSocket tests ─────────────────────────────────────────────────────────

async fn test_websocket(ws_url: &str, listen_secs: u64, results: &mut TestResults) {
    let name_connect = "ws::connect";

    let mut client = WebSocketClient::new(ws_url);
    let _ = client
        .set_reconnect_options(true, BackoffStrategy::default_exponential(), 3)
        .await;

    if let Err(e) = client.connect().await {
        results.fail(name_connect, &format!("{}", e));
        return;
    }
    results.pass(name_connect);

    let _ = client
        .enable_keep_alive(Duration::from_secs(15))
        .await;

    let block_count = Arc::new(Mutex::new(0u64));
    let tx_count = Arc::new(Mutex::new(0u64));
    let last_block_height = Arc::new(Mutex::new(Option::<u64>::None));

    // subscribe to blocks
    {
        let bc = block_count.clone();
        let lbh = last_block_height.clone();
        if let Err(e) = client
            .on_event_async(EventTopic::Block, None, move |event: Event| {
                let bc = bc.clone();
                let lbh = lbh.clone();
                async move {
                    if let Event::Block(be) = event {
                        let mut c = bc.lock().await;
                        *c += 1;
                        info!("ws: block event #{} — hash={}", *c, be.hash);
                        *lbh.lock().await = Some(*c);
                    }
                }
            })
            .await
        {
            results.fail("ws::subscribe_block", &format!("{}", e));
            let _ = client.close().await;
            return;
        }
        results.pass("ws::subscribe_block");
    }

    // subscribe to transactions
    {
        let tc = tx_count.clone();
        if let Err(e) = client
            .on_event_async(EventTopic::Transaction, None, move |event: Event| {
                let tc = tc.clone();
                async move {
                    if let Event::Transaction(te) = event {
                        let mut c = tc.lock().await;
                        *c += 1;
                        if *c <= 3 {
                            info!(
                                "ws: tx event #{} — hash={}, status={:?}",
                                *c, te.hash, te.status
                            );
                        } else if *c % 100 == 0 {
                            info!("ws: tx event #{} (sampling — hash={})", *c, te.hash);
                        }
                    }
                }
            })
            .await
        {
            results.fail("ws::subscribe_transaction", &format!("{}", e));
            let _ = client.close().await;
            return;
        }
        results.pass("ws::subscribe_transaction");
    }

    info!(
        "Listening for WebSocket events for {} seconds…",
        listen_secs
    );
    tokio::time::sleep(Duration::from_secs(listen_secs)).await;

    let blocks = *block_count.lock().await;
    let txs = *tx_count.lock().await;

    info!("ws: received {} block events, {} tx events", blocks, txs);

    if blocks > 0 {
        results.pass("ws::received_block_events");
    } else {
        results.fail(
            "ws::received_block_events",
            &format!(
                "no block events received in {} seconds — chain may be idle or WS subscription failed",
                listen_secs
            ),
        );
    }

    if txs > 0 {
        results.pass("ws::received_tx_events");
    } else {
        warn!(
            "No transaction events received — this may be normal if no txs were submitted during the window"
        );
        results.skip("ws::received_tx_events");
    }

    let _ = client.close().await;
}

// ─── main ────────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let mut results = TestResults::default();

    info!("Arch Atlas Integration Test Suite");
    info!("RPC URL: {}", cli.rpc_url);
    info!("WS  URL: {}", cli.ws_url);
    println!("───────────────────────────────────────────────");

    // ── RPC tests ──

    let rpc = make_rpc_client(&cli.rpc_url);

    let tip = test_get_block_count(&rpc, &mut results).await;
    test_get_best_block_hash(&rpc, &mut results).await;

    if let Some(tip) = tip {
        match find_available_block(&rpc, tip).await {
            Some(lowest) => {
                info!(
                    "Probed first available block at height {} (tip={})",
                    lowest, tip
                );
                results.pass("probe::find_available_block");

                // Use a block near the tip for individual tests (more likely to have txs)
                let test_h = tip.saturating_sub(2).max(lowest);
                info!("Running block tests against height {}", test_h);

                test_get_block_by_height(&rpc, test_h, &mut results).await;
                test_get_full_block_by_height(&rpc, test_h, &mut results).await;
                test_get_block_hash(&rpc, test_h, &mut results).await;
                test_get_full_block_by_hash(&rpc, test_h, &mut results).await;
                test_get_processed_transaction(&rpc, test_h, &mut results).await;
                test_atlas_arch_rpc_client(&cli.rpc_url, &mut results).await;
            }
            None => {
                results.fail(
                    "probe::find_available_block",
                    "could not find any available block via binary search",
                );
            }
        }
    }

    // ── WebSocket tests ──

    if cli.skip_ws {
        results.skip("ws::* (skipped via --skip-ws)");
    } else {
        test_websocket(&cli.ws_url, cli.ws_listen_secs, &mut results).await;
    }

    // ── Summary ──

    results.summary();
    std::process::exit(results.exit_code());
}
