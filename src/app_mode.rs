//! 앱 모드: 인자 없이 실행(= Finder에서 더블클릭)하면 터미널 없이 쓰는 흐름.
//!
//! 1. 이미 켜져 있으면 브라우저만 열고 끝난다(중복 실행 방지).
//! 2. 저장된 방이 있으면 그 방으로 바로 접속하고 단톡방 화면을 연다.
//! 3. 없으면 시작 화면에서 [방 만들기] 또는 [초대 코드 넣기]를 받고, 저장한 뒤 2로 간다.
//!
//! 시험·여러 개 띄우기용 환경 변수: `AMP2P_HOME`(데이터 폴더), `AMP2P_PORT`(기본 18765),
//! `AMP2P_NO_BROWSER=1`(브라우저 열지 않기).

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router as AxumRouter};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::oneshot;

use crate::layer1_transport::SwarmTicket;
use crate::layer5_mcp_bridge::LocalBridgeState;

const START_PAGE_HTML: &str = include_str!("../ui/start.html");

/// 실행 중인 노드가 화면에 알려 주는 정보.
#[derive(Clone, Default)]
pub struct AppInfo {
    pub app_mode: bool,
    pub role: String,
    pub name: String,
    pub ticket: String,
    pub port: u16,
    pub config_path: Option<PathBuf>,
}

/// 앱이 다음 실행 때 같은 방으로 들어가기 위해 저장하는 설정.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    /// "host"(방을 만든 사람) 또는 "member"(초대받은 사람)
    pub role: String,
    pub agent: String,
    pub name: String,
    /// host만: 방(가십 토픽) 식별자. 재시작해도 같은 방을 유지한다.
    #[serde(default)]
    pub topic_hex: Option<String>,
    /// member만: 받은 초대 코드
    #[serde(default)]
    pub ticket: Option<String>,
}

pub fn app_home() -> PathBuf {
    if let Ok(h) = std::env::var("AMP2P_HOME") {
        return PathBuf::from(h);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join("Library/Application Support/AgentMail")
}

pub fn app_port() -> u16 {
    std::env::var("AMP2P_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(18765)
}

fn config_path(home: &Path) -> PathBuf {
    home.join("config.json")
}

fn load_config(home: &Path) -> Option<AppConfig> {
    let raw = std::fs::read_to_string(config_path(home)).ok()?;
    serde_json::from_str(&raw).ok()
}

fn save_config(home: &Path, cfg: &AppConfig) -> Result<()> {
    std::fs::create_dir_all(home)?;
    std::fs::write(config_path(home), serde_json::to_vec_pretty(cfg)?)?;
    Ok(())
}

fn port_in_use(port: u16) -> bool {
    std::net::TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(300),
    )
    .is_ok()
}

fn open_browser(port: u16) {
    if std::env::var("AMP2P_NO_BROWSER").as_deref() == Ok("1") {
        return;
    }
    let url = format!("http://127.0.0.1:{port}/");
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg(&url).spawn();
    #[cfg(target_os = "windows")]
    let _ = std::process::Command::new("cmd").args(["/C", "start", "", &url]).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let _ = std::process::Command::new("xdg-open").arg(&url).spawn();
}

/// 서버가 응답하기 시작하면 브라우저를 한 번 연다.
fn open_browser_when_ready(port: u16) {
    tokio::spawn(async move {
        for _ in 0..100 {
            if port_in_use(port) {
                open_browser(port);
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}

fn random_member_id() -> String {
    const ALPHA: &[u8] = b"abcdefghjkmnpqrstuvwxyz23456789";
    let bytes: [u8; 4] = rand::random();
    let tail: String = bytes.iter().map(|b| ALPHA[*b as usize % ALPHA.len()] as char).collect();
    format!("m{tail}")
}

/// 앱 모드 진입점.
pub async fn run() -> Result<()> {
    let port = app_port();
    let home = app_home();

    if std::env::var("AMP2P_WAIT_PORT").as_deref() == Ok("1") {
        // "다른 방으로 옮기기" 직후 다시 실행된 경우: 이전 프로세스가 포트를 놓을 때까지 기다린다.
        for _ in 0..80 {
            if !port_in_use(port) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    } else if port_in_use(port) {
        open_browser(port);
        return Ok(());
    }

    std::fs::create_dir_all(&home).with_context(|| format!("cannot create {}", home.display()))?;
    open_browser_when_ready(port);

    let cfg = match load_config(&home) {
        Some(cfg) => cfg,
        None => {
            let cfg = run_start_page(port).await?;
            save_config(&home, &cfg)?;
            cfg
        }
    };

    let info = AppInfo {
        app_mode: true,
        role: cfg.role.clone(),
        name: cfg.name.clone(),
        ticket: String::new(),
        port,
        config_path: Some(config_path(&home)),
    };
    // 방마다 대화 기록 폴더를 따로 둔다(다른 방으로 옮겨도 섞이지 않게).
    if cfg.role == "host" {
        let topic_hex = cfg.topic_hex.clone().context("host config without topic")?;
        let decoded = data_encoding::HEXLOWER
            .decode(topic_hex.as_bytes())
            .context("bad topic in config")?;
        let mut topic = [0u8; 32];
        topic.copy_from_slice(&decoded[..32]);
        let data_dir = home.join("data").join(&topic_hex[..12]);
        crate::start_host(cfg.agent, cfg.name, topic, port, data_dir, info).await
    } else {
        let ticket = cfg.ticket.clone().context("member config without ticket")?;
        let parsed = SwarmTicket::from_str(&ticket)?;
        let room = data_encoding::HEXLOWER.encode(&parsed.topic.as_bytes()[..6]);
        let data_dir = home.join("data").join(room);
        crate::start_member(ticket, cfg.agent, cfg.name, "*".into(), port, data_dir, info).await
    }
}

// ---------------------------------------------------------------------------
// 시작 화면 (방이 아직 없을 때만 뜬다)
// ---------------------------------------------------------------------------

struct StartState {
    done: Mutex<Option<oneshot::Sender<AppConfig>>>,
}

#[derive(Deserialize)]
struct CreateBody {
    name: String,
}

#[derive(Deserialize)]
struct JoinBody {
    name: String,
    code: String,
}

fn clean_name(raw: &str) -> Option<String> {
    let n: String = raw.trim().chars().take(20).collect();
    if n.is_empty() {
        None
    } else {
        Some(n)
    }
}

async fn run_start_page(port: u16) -> Result<AppConfig> {
    let (tx, rx) = oneshot::channel::<AppConfig>();
    let state = Arc::new(StartState { done: Mutex::new(Some(tx)) });
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let (cfg_tx, cfg_rx) = oneshot::channel::<AppConfig>();

    tokio::spawn(async move {
        if let Ok(cfg) = rx.await {
            // 응답이 브라우저에 도착할 시간을 조금 준 뒤 시작 화면 서버를 내린다.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = cfg_tx.send(cfg);
            let _ = stop_tx.send(());
        }
    });

    let app = AxumRouter::new()
        .route("/", get(|| async { Html(START_PAGE_HTML) }))
        .route("/api/app/info", get(|| async { Json(json!({ "stage": "start" })) }))
        .route("/api/app/create", post(start_create))
        .route("/api/app/join", post(start_join))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = stop_rx.await;
        })
        .await?;
    cfg_rx.await.context("start page closed without a choice")
}

fn finish(st: &StartState, cfg: AppConfig) -> Json<Value> {
    if let Some(tx) = st.done.lock().unwrap().take() {
        let _ = tx.send(cfg);
        Json(json!({ "ok": true }))
    } else {
        Json(json!({ "ok": false, "error": "이미 처리 중이에요. 잠시 기다려 주세요." }))
    }
}

async fn start_create(State(st): State<Arc<StartState>>, Json(b): Json<CreateBody>) -> Json<Value> {
    let Some(name) = clean_name(&b.name) else {
        return Json(json!({ "ok": false, "error": "이름을 적어 주세요." }));
    };
    let topic: [u8; 32] = rand::random();
    finish(
        &st,
        AppConfig {
            role: "host".into(),
            agent: "pm".into(),
            name,
            topic_hex: Some(data_encoding::HEXLOWER.encode(&topic)),
            ticket: None,
        },
    )
}

async fn start_join(State(st): State<Arc<StartState>>, Json(b): Json<JoinBody>) -> Json<Value> {
    let Some(name) = clean_name(&b.name) else {
        return Json(json!({ "ok": false, "error": "이름을 적어 주세요." }));
    };
    let code: String = b.code.split_whitespace().collect();
    if SwarmTicket::from_str(&code).is_err() {
        return Json(json!({
            "ok": false,
            "error": "초대 코드가 올바르지 않아요. 받은 코드를 처음부터 끝까지 그대로 붙여 넣어 주세요."
        }));
    }
    finish(
        &st,
        AppConfig {
            role: "member".into(),
            agent: random_member_id(),
            name,
            topic_hex: None,
            ticket: Some(code),
        },
    )
}

// ---------------------------------------------------------------------------
// 실행 중인 노드의 앱 API (단톡방 화면의 버튼들)
// ---------------------------------------------------------------------------

fn mcp_args(st: &LocalBridgeState) -> Vec<String> {
    let mut args = vec!["mcp".to_string(), "--agent".to_string(), st.agent_id.clone()];
    if st.app.port != 18765 {
        args.push("--daemon-url".into());
        args.push(format!("http://127.0.0.1:{}", st.app.port));
    }
    args
}

fn current_exe_string() -> String {
    std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "agent-mail-p2p".into())
}

fn shell_quote(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_alphanumeric() || "-_./:".contains(c)) {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

fn claude_command_line(st: &LocalBridgeState) -> String {
    let mut parts = vec![
        "claude".to_string(),
        "mcp".into(),
        "add".into(),
        "-s".into(),
        "user".into(),
        "agent-mail".into(),
        "--".into(),
        shell_quote(&current_exe_string()),
    ];
    parts.extend(mcp_args(st));
    parts.join(" ")
}

pub async fn api_app_info(State(st): State<Arc<LocalBridgeState>>) -> Json<Value> {
    Json(json!({
        "stage": "running",
        "app_mode": st.app.app_mode,
        "version": env!("CARGO_PKG_VERSION"),
        "agent": st.agent_id,
        "name": st.app.name,
        "role": st.app.role,
        "is_host": st.is_overseer,
        "invite_code": st.app.ticket,
        "claude_command": claude_command_line(&st),
    }))
}

/// Finder에서 실행된 앱은 터미널의 PATH를 모르므로, 흔한 설치 위치와 로그인 셸에서 claude를 찾는다.
fn find_claude() -> Option<PathBuf> {
    let home = std::env::var("HOME").unwrap_or_default();
    let candidates = [
        format!("{home}/.local/bin/claude"),
        format!("{home}/.claude/local/claude"),
        "/opt/homebrew/bin/claude".to_string(),
        "/usr/local/bin/claude".to_string(),
    ];
    for c in candidates {
        if Path::new(&c).is_file() {
            return Some(PathBuf::from(c));
        }
    }
    let out = std::process::Command::new("/bin/zsh")
        .args(["-lc", "command -v claude"])
        .output()
        .ok()?;
    let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if !p.is_empty() && Path::new(&p).is_file() {
        Some(PathBuf::from(p))
    } else {
        None
    }
}

pub async fn api_connect_claude(State(st): State<Arc<LocalBridgeState>>) -> Json<Value> {
    let command = claude_command_line(&st);
    let Some(claude) = find_claude() else {
        return Json(json!({
            "ok": false,
            "reason": "no_claude",
            "message": "이 맥에서 Claude Code를 찾지 못했어요. Claude Code를 설치했다면 아래 명령을 터미널에 붙여 넣어 주세요.",
            "command": command,
        }));
    };
    // 이미 등록돼 있으면 add가 실패하므로 먼저 지운다(없으면 무시).
    let _ = std::process::Command::new(&claude)
        .args(["mcp", "remove", "agent-mail", "-s", "user"])
        .output();
    let mut add_args: Vec<String> = ["mcp", "add", "-s", "user", "agent-mail", "--"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    add_args.push(current_exe_string());
    add_args.extend(mcp_args(&st));
    match std::process::Command::new(&claude).args(&add_args).output() {
        Ok(out) if out.status.success() => Json(json!({
            "ok": true,
            "message": "Claude Code에 연결했어요. Claude Code를 새로 열면 단톡방 도구가 보여요.",
        })),
        Ok(out) => Json(json!({
            "ok": false,
            "reason": "add_failed",
            "message": "연결하지 못했어요. 아래 명령을 터미널에 붙여 넣어 주세요.",
            "detail": String::from_utf8_lossy(&out.stderr).trim().to_string(),
            "command": command,
        })),
        Err(e) => Json(json!({
            "ok": false,
            "reason": "spawn_failed",
            "message": "연결하지 못했어요. 아래 명령을 터미널에 붙여 넣어 주세요.",
            "detail": e.to_string(),
            "command": command,
        })),
    }
}

fn exit_soon() {
    tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        std::process::exit(0);
    });
}

pub async fn api_quit(State(st): State<Arc<LocalBridgeState>>) -> Json<Value> {
    if !st.app.app_mode {
        return Json(json!({ "ok": false, "error": "앱으로 실행한 경우에만 끌 수 있어요." }));
    }
    exit_soon();
    Json(json!({ "ok": true }))
}

/// 저장된 방을 지우고 앱을 다시 실행해 시작 화면으로 돌아간다(대화 기록은 data 폴더에 남는다).
pub async fn api_leave(State(st): State<Arc<LocalBridgeState>>) -> Json<Value> {
    if !st.app.app_mode {
        return Json(json!({ "ok": false, "error": "앱으로 실행한 경우에만 쓸 수 있어요." }));
    }
    if let Some(p) = &st.app.config_path {
        let _ = std::fs::remove_file(p);
    }
    if let Ok(exe) = std::env::current_exe() {
        let _ = std::process::Command::new(exe)
            .env("AMP2P_WAIT_PORT", "1")
            .env("AMP2P_NO_BROWSER", "1")
            .spawn();
    }
    exit_soon();
    Json(json!({ "ok": true }))
}
