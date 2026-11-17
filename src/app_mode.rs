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

/// ~/.claude/settings.json에 받기용 훅 3개를 넣는다. 다른 훅은 건드리지 않고,
/// 예전에 넣은 agent-mail 훅만 바꾼다. 쓰기 전에 원본을 백업한다.
pub fn install_claude_hooks(agent: &str, port: u16) -> Result<String> {
    let home = std::env::var("HOME").context("HOME not set")?;
    let path = PathBuf::from(&home).join(".claude/settings.json");
    let mut root: Value = match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw).context("~/.claude/settings.json이 올바른 JSON이 아니에요")?,
        Err(_) => json!({}),
    };
    if path.exists() {
        std::fs::copy(&path, path.with_extension("json.agent-mail-backup"))?;
    }
    let exe = shell_quote(&current_exe_string());
    let url = format!("http://127.0.0.1:{port}");
    let cmd = |mode: &str| format!("{exe} hook {mode} --agent {agent} --daemon-url {url}");
    let ours = |h: &Value| {
        h.get("command").and_then(|c| c.as_str()).map(|c| c.contains(" hook post-tool ") || c.contains(" hook prompt ") || c.contains(" hook stop-wait ") || c.contains(" hook session-start ") || c.contains(" hook listen ") || c.contains(" hook pre-tool ")).unwrap_or(false)
            && h.get("command").and_then(|c| c.as_str()).map(|c| c.contains("agent-mail")).unwrap_or(false)
    };
    let hooks = root.as_object_mut().context("settings.json 최상위가 객체가 아니에요")?
        .entry("hooks").or_insert_with(|| json!({}));
    let wanted = [
        // 도구 실행 직전: 듣기 명령을 앞쪽에서 돌리는 실수를 막고, 안내를 계속 무시하면 듣기부터 켜게 한다.
        ("PreToolUse", json!({ "type": "command", "command": cmd("pre-tool"), "timeout": 10 })),
        ("PostToolUse", json!({ "type": "command", "command": cmd("post-tool"), "timeout": 10 })),
        ("UserPromptSubmit", json!({ "type": "command", "command": cmd("prompt"), "timeout": 10 })),
        // 세션을 열면 꺼진 방을 다시 켜고, 방 상황을 들고 Claude가 먼저 말을 건다.
        ("SessionStart", json!({ "type": "command", "command": cmd("session-start"), "asyncRewake": true, "timeout": 60 })),
        // 메일 듣기는 세션마다 항상 하나 살아 있게: 시작·말할 때·답을 마칠 때 확인하고 없으면 띄운다.
        ("SessionStart", json!({ "type": "command", "command": cmd("listen"), "asyncRewake": true, "timeout": 86400 })),
        ("UserPromptSubmit", json!({ "type": "command", "command": cmd("listen"), "asyncRewake": true, "timeout": 86400 })),
        ("Stop", json!({ "type": "command", "command": cmd("listen"), "asyncRewake": true, "timeout": 86400 })),
    ];
    // 1) 예전에 넣은 agent-mail 훅을 모든 이벤트에서 먼저 지운다(다른 훅은 그대로).
    let hooks_obj = hooks.as_object_mut().context("hooks가 객체가 아니에요")?;
    for (_event, groups) in hooks_obj.iter_mut() {
        if let Some(arr) = groups.as_array_mut() {
            for g in arr.iter_mut() {
                if let Some(list) = g.get_mut("hooks").and_then(|l| l.as_array_mut()) {
                    list.retain(|h| !ours(h));
                }
            }
            arr.retain(|g| g.get("hooks").and_then(|l| l.as_array()).map(|l| !l.is_empty()).unwrap_or(true));
        }
    }
    // 2) 새 훅을 넣는다(같은 이벤트에 여러 개여도 모두 남는다).
    for (event, handler) in wanted {
        let groups = hooks_obj.entry(event).or_insert_with(|| json!([]));
        let arr = groups.as_array_mut().context("hook 목록이 배열이 아니에요")?;
        arr.push(json!({ "hooks": [handler] }));
    }
    // 3) 사용자가 연결을 허락했으니 agent-mail 도구와 듣기 명령은 Claude 자동 승인이 다시 막지 않게 허용 목록에 넣는다.
    //    (이 두 가지만. 다른 권한은 건드리지 않는다.) 사람 입력 없이 깨어난 AI가 답장하지 못하던 문제를 푼다.
    let exe_raw = current_exe_string();
    let wanted_allow = vec![
        "mcp__agent-mail".to_string(),
        format!("Bash({exe_raw} wait:*)"),
    ];
    let perms = root.as_object_mut().context("settings.json 최상위가 객체가 아니에요")?
        .entry("permissions").or_insert_with(|| json!({}));
    let allow = perms.as_object_mut().context("permissions가 객체가 아니에요")?
        .entry("allow").or_insert_with(|| json!([]));
    let arr = allow.as_array_mut().context("permissions.allow가 배열이 아니에요")?;
    arr.retain(|v| {
        let s = v.as_str().unwrap_or("");
        !(s == "mcp__agent-mail" || s.starts_with("mcp__agent-mail__") || (s.starts_with("Bash(") && s.contains("agent-mail-p2p wait")))
    });
    for a in wanted_allow {
        arr.push(json!(a));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&root)?)?;
    Ok(path.display().to_string())
}

/// MCP 등록 + 받기용 훅 설치를 한 번에 한다(앱 버튼과 CLI `connect-claude`가 같이 쓴다).
pub fn connect_claude(agent: &str, port: u16) -> Value {
    let mut args = vec!["mcp".to_string(), "--agent".to_string(), agent.to_string()];
    if port != 18765 {
        args.push("--daemon-url".into());
        args.push(format!("http://127.0.0.1:{port}"));
    }
    let mut parts = vec!["claude", "mcp", "add", "-s", "user", "agent-mail", "--"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
    parts.push(shell_quote(&current_exe_string()));
    parts.extend(args.iter().cloned());
    let command = parts.join(" ");

    let hooks = install_claude_hooks(agent, port);
    let Some(claude) = find_claude() else {
        return json!({
            "ok": false, "reason": "no_claude",
            "message": "이 컴퓨터에서 Claude Code를 찾지 못했어요. 설치했다면 아래 명령을 터미널에 붙여 넣어 주세요.",
            "command": command,
            "hooks_installed": hooks.is_ok(),
        });
    };
    let _ = std::process::Command::new(&claude).args(["mcp", "remove", "agent-mail", "-s", "user"]).output();
    let mut add_args: Vec<String> = ["mcp", "add", "-s", "user", "agent-mail", "--"].iter().map(|s| s.to_string()).collect();
    add_args.push(current_exe_string());
    add_args.extend(args);
    let added = std::process::Command::new(&claude).args(&add_args).output();
    match (added, hooks) {
        (Ok(out), Ok(settings)) if out.status.success() => json!({
            "ok": true,
            "message": "Claude Code에 연결했어요. Claude Code를 새로 열면 단톡방 메일을 받을 수 있어요 (쉬고 있어도 새 메일이 오면 깨어나요).",
            "settings": settings,
        }),
        (Ok(out), Ok(_)) => json!({
            "ok": false, "reason": "add_failed",
            "message": "받기 설정은 했지만 도구 연결에 실패했어요. 아래 명령을 터미널에 붙여 넣어 주세요.",
            "detail": String::from_utf8_lossy(&out.stderr).trim().to_string(), "command": command,
        }),
        (_, Err(e)) => json!({
            "ok": false, "reason": "hooks_failed",
            "message": "Claude 설정 파일을 고치지 못했어요.", "detail": e.to_string(), "command": command,
        }),
        (Err(e), _) => json!({
            "ok": false, "reason": "spawn_failed",
            "message": "연결하지 못했어요. 아래 명령을 터미널에 붙여 넣어 주세요.", "detail": e.to_string(), "command": command,
        }),
    }
}

pub async fn api_connect_claude(State(st): State<Arc<LocalBridgeState>>) -> Json<Value> {
    Json(connect_claude(&st.agent_id, st.app.port))
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
