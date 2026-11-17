//! Layer 5: Hybrid Two-Track Agent Last-Mile (stdio MCP `notifications/claude/channel` + Lifecycle Hooks + Local Axum UI)
//!
//! - 각 팀원 PC의 P2P 데몬이 로컬 `127.0.0.1:18765` HTTP/SSE 인터페이스를 열어
//!   웹 단톡방 UI(`include_str!`로 단일 바이너리에 내장), 로컬 `mcp` 서브프로세스,
//!   `PostToolUse`/`PreToolUse`/`Stop` 훅을 외부 파일 없이 연결합니다.
//! - Track 1: 긴급(`urgent`) 메시지가 로컬 SQLite에 원자적으로 커밋되는 즉시
//!   `notifications/claude/channel`을 통해 실행 중인 Claude Code 세션에 1줄 요약 힌트를 푸시합니다.
//! - Track 2: 일반 메시지와 타 호스트(Codex, Cursor, Gemini)는 `/api/wait` 롱폴링 및 0ms 훅으로 전달합니다.

use std::convert::Infallible;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::{Query, State};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router as AxumRouter};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::layer2_reconcile::{GossipKind, MailHeader, MailPriority, INLINE_BODY_MAX_BYTES};
use crate::layer3_state_auth::{now_unix_ms, HybridClock, LeaseClaim};
use crate::layer4_sqlite_view::SqliteProjection;

const EMBEDDED_GROUP_CHAT_HTML: &str = include_str!("../ui/index.html");

pub struct LocalBridgeState {
    pub agent_id: String,
    pub node_pk: [u8; 32],
    pub is_overseer: bool,
    pub store: Arc<SqliteProjection>,
    pub clock: std::sync::Mutex<HybridClock>,
    pub gossip_out_tx: mpsc::Sender<GossipKind>,
}

#[derive(Deserialize)]
pub struct WaitParams {
    pub agent: String,
    #[serde(default)]
    pub urgent_only: u8,
    #[serde(default)]
    pub timeout: u64,
}

#[derive(Deserialize)]
pub struct CheckParams {
    pub agent: String,
    pub path: String,
}

#[derive(Deserialize)]
pub struct SendBody {
    pub sender: Option<String>,
    pub recipient: String,
    pub hint: Option<String>,
    pub body_md: String,
    pub priority: Option<String>,
}

#[derive(Deserialize)]
pub struct LeaseBody {
    pub agent_id: String,
    pub glob_pattern: String,
    pub ttl_seconds: Option<u64>,
    pub reason: Option<String>,
}

pub fn build_local_http_router(state: Arc<LocalBridgeState>) -> AxumRouter {
    AxumRouter::new()
        .route("/", get(serve_embedded_ui))
        .route("/api/state", get(api_state))
        .route("/api/wait", get(api_wait))
        .route("/api/leases/check", get(api_lease_check))
        .route("/api/events", get(api_sse_events))
        .route("/api/send", post(api_send))
        .route("/api/leases/reserve", post(api_lease_reserve))
        .with_state(state)
}

async fn serve_embedded_ui() -> Html<&'static str> {
    Html(EMBEDDED_GROUP_CHAT_HTML)
}

async fn api_state(State(st): State<Arc<LocalBridgeState>>) -> Json<Value> {
    Json(st.store.full_state_json())
}

async fn api_lease_check(
    State(st): State<Arc<LocalBridgeState>>,
    Query(q): Query<CheckParams>,
) -> Json<Value> {
    if let Some(conflict) = st.store.check_path_conflict(&q.agent, &q.path) {
        Json(json!({
            "allowed": false,
            "holder_agent": conflict.holder_agent,
            "glob_pattern": conflict.glob_pattern,
            "reason": format!(
                "[P2P 리스 가드] '{}' 경로는 '{}' 팀원이 점유 중입니다 (패턴: {}).",
                q.path, conflict.holder_agent, conflict.glob_pattern
            )
        }))
    } else {
        Json(json!({ "allowed": true }))
    }
}

async fn api_wait(
    State(st): State<Arc<LocalBridgeState>>,
    Query(q): Query<WaitParams>,
) -> Json<Value> {
    let urgent_only = q.urgent_only == 1;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(q.timeout.min(30));

    loop {
        let notified = st.store.wake_notify.notified();
        let hints = st.store.drain_pending_hints(&q.agent, urgent_only);
        if !hints.is_empty() || q.timeout == 0 {
            return Json(json!({ "ok": true, "hints": hints }));
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Json(json!({ "ok": true, "hints": [] }));
        }
        if tokio::time::timeout(deadline - now, notified).await.is_err() {
            return Json(json!({ "ok": true, "hints": [] }));
        }
    }
}

async fn api_sse_events(
    State(st): State<Arc<LocalBridgeState>>,
) -> Sse<impl tokio_stream::Stream<Item = std::result::Result<SseEvent, Infallible>>> {
    let rx = st.store.event_tx.subscribe();
    let store_clone = st.store.clone();
    let stream = BroadcastStream::new(rx).filter_map(move |res| {
        res.ok().map(|_ev| {
            let snap = store_clone.full_state_json().to_string();
            Ok(SseEvent::default().data(snap))
        })
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn api_send(
    State(st): State<Arc<LocalBridgeState>>,
    Json(body): Json<SendBody>,
) -> Json<Value> {
    let sender = body.sender.unwrap_or_else(|| st.agent_id.clone());
    if sender == "OVERSEER" && !st.is_overseer {
        return Json(json!({ "ok": false, "error": "현재 노드는 Overseer 루트 권한 인증서가 없습니다." }));
    }

    let hlc = st.clock.lock().unwrap().tick();
    let msg_id = format!("msg_{}_{}", hlc.wall_ms, hlc.logical);
    let prio = if body.priority.as_deref() == Some("urgent") {
        MailPriority::Urgent
    } else {
        MailPriority::Normal
    };
    let hint = body
        .hint
        .unwrap_or_else(|| body.body_md.lines().next().unwrap_or("").chars().take(120).collect());
    let body_hash = *blake3::hash(body.body_md.as_bytes()).as_bytes();
    let inline_body = if body.body_md.len() <= INLINE_BODY_MAX_BYTES {
        Some(body.body_md.clone())
    } else {
        None
    };

    let hdr = MailHeader {
        id: msg_id.clone(),
        sender_agent: sender,
        recipient: body.recipient,
        priority: prio,
        hint,
        body_blake3: body_hash,
        body_len: body.body_md.len() as u32,
        inline_body,
        hlc,
    };

    let _ = st.store.commit_verified_mail(&hdr, &body.body_md);
    let _ = st.gossip_out_tx.send(GossipKind::MailPublished(hdr)).await;

    Json(json!({ "ok": true, "id": msg_id }))
}

async fn api_lease_reserve(
    State(st): State<Arc<LocalBridgeState>>,
    Json(body): Json<LeaseBody>,
) -> Json<Value> {
    let hlc = st.clock.lock().unwrap().tick();
    let ttl_ms = body.ttl_seconds.unwrap_or(3600) * 1000;
    let claim = LeaseClaim {
        glob_pattern: body.glob_pattern.clone(),
        holder_agent: body.agent_id,
        holder_pk: st.node_pk,
        acquired_hlc: hlc,
        expires_wall_ms: now_unix_ms() + ttl_ms,
        reason: body.reason.unwrap_or_default(),
        overseer_override: st.is_overseer,
    };

    match st.store.apply_lease_claim(claim.clone()) {
        Ok(()) => {
            let _ = st.gossip_out_tx.send(GossipKind::LeaseAnnounced(claim)).await;
            Json(json!({ "ok": true, "glob_pattern": body.glob_pattern }))
        }
        Err(winner) => Json(json!({
            "ok": false,
            "conflict": true,
            "holder_agent": winner.holder_agent,
            "glob_pattern": winner.glob_pattern
        })),
    }
}

/// stdio MCP JSON-RPC 2.0 서버 + 백그라운드 `notifications/claude/channel` 실시간 푸시 루프
pub fn run_stdio_mcp_bridge(daemon_url: &str, agent_id: &str, enable_channel_push: bool) -> Result<()> {
    let stdout_lock = Arc::new(std::sync::Mutex::new(()));

    if enable_channel_push {
        let base = daemon_url.to_string();
        let aid = agent_id.to_string();
        let out_lk = stdout_lock.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                loop {
                    let url = format!("{base}/api/wait?agent={aid}&urgent_only=1&timeout=20");
                    if let Ok(resp) = reqwest_free_http_get(&url) {
                        if let Some(hints) = resp.get("hints").and_then(|v| v.as_array()) {
                            for h in hints {
                                let sender = h.get("sender").and_then(|v| v.as_str()).unwrap_or("peer");
                                let hint_txt = h.get("hint").and_then(|v| v.as_str()).unwrap_or("");
                                let mid = h.get("id").and_then(|v| v.as_str()).unwrap_or("");
                                let notif = json!({
                                    "jsonrpc": "2.0",
                                    "method": "notifications/claude/channel",
                                    "params": {
                                        "content": format!("[P2P 긴급 @{sender}] {hint_txt} (fetch_inbox 호출, id={mid})"),
                                        "meta": {
                                            "msg_id": mid,
                                            "sender": sender,
                                            "priority": "urgent"
                                        }
                                    }
                                });
                                let _g = out_lk.lock().unwrap();
                                let _ = writeln!(io::stdout(), "{notif}");
                                let _ = io::stdout().flush();
                            }
                        }
                    } else {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            });
        });
    }

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(req) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let req_id = req.get("id").cloned();

        if method == "initialize" {
            let mut caps = json!({ "tools": {} });
            if enable_channel_push {
                caps["experimental"] = json!({ "claude/channel": {} });
            }
            let resp = json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": caps,
                    "serverInfo": { "name": format!("agent-mail-p2p-{agent_id}"), "version": "0.1.0" }
                }
            });
            let _g = stdout_lock.lock().unwrap();
            writeln!(io::stdout(), "{resp}")?;
            io::stdout().flush()?;
        } else if method == "tools/list" {
            let resp = json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [
                        { "name": "send_mail", "description": "P2P 스웜으로 메시지 발송", "inputSchema": { "type": "object", "properties": { "recipient": {"type":"string"}, "body_md": {"type":"string"}, "priority": {"type":"string"} }, "required": ["recipient","body_md"] } },
                        { "name": "reserve_files", "description": "HLC 결정론적 배타 파일 리스 요청", "inputSchema": { "type": "object", "properties": { "glob_pattern": {"type":"string"} }, "required": ["glob_pattern"] } },
                        { "name": "team_board_status", "description": "5인 P2P 작업 분담 및 리스 상태 조회", "inputSchema": { "type": "object", "properties": {} } }
                    ]
                }
            });
            let _g = stdout_lock.lock().unwrap();
            writeln!(io::stdout(), "{resp}")?;
            io::stdout().flush()?;
        } else if method == "tools/call" {
            let params = req.get("params").cloned().unwrap_or_default();
            let t_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or_default();
            let out = match t_name {
                "team_board_status" => reqwest_free_http_get(&format!("{daemon_url}/api/state")).unwrap_or_default(),
                "send_mail" => reqwest_free_http_post(
                    &format!("{daemon_url}/api/send"),
                    &json!({
                        "sender": agent_id,
                        "recipient": args.get("recipient").and_then(|v| v.as_str()).unwrap_or("ALL"),
                        "body_md": args.get("body_md").and_then(|v| v.as_str()).unwrap_or(""),
                        "priority": args.get("priority").and_then(|v| v.as_str()).unwrap_or("normal"),
                    }),
                )
                .unwrap_or_default(),
                "reserve_files" => reqwest_free_http_post(
                    &format!("{daemon_url}/api/leases/reserve"),
                    &json!({
                        "agent_id": agent_id,
                        "glob_pattern": args.get("glob_pattern").and_then(|v| v.as_str()).unwrap_or(""),
                    }),
                )
                .unwrap_or_default(),
                _ => json!({"ok": false, "error": "unknown tool"}),
            };
            let resp = json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": { "content": [{ "type": "text", "text": out.to_string() }] }
            });
            let _g = stdout_lock.lock().unwrap();
            writeln!(io::stdout(), "{resp}")?;
            io::stdout().flush()?;
        }
    }
    Ok(())
}

pub fn reqwest_free_http_get(url: &str) -> Result<Value> {
    let stripped = url.strip_prefix("http://").unwrap_or(url);
    let (host_port, path) = stripped.split_once('/').unwrap_or((stripped, ""));
    let mut stream = std::net::TcpStream::connect(host_port)?;
    write!(
        stream,
        "GET /{path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n"
    )?;
    let mut resp = String::new();
    io::Read::read_to_string(&mut stream, &mut resp)?;
    let body = resp.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("{}");
    Ok(serde_json::from_str(body)?)
}

pub fn reqwest_free_http_post(url: &str, payload: &Value) -> Result<Value> {
    let stripped = url.strip_prefix("http://").unwrap_or(url);
    let (host_port, path) = stripped.split_once('/').unwrap_or((stripped, ""));
    let raw = payload.to_string();
    let mut stream = std::net::TcpStream::connect(host_port)?;
    write!(
        stream,
        "POST /{path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        raw.len(),
        raw
    )?;
    let mut resp = String::new();
    io::Read::read_to_string(&mut stream, &mut resp)?;
    let body = resp.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("{}");
    Ok(serde_json::from_str(body)?)
}
