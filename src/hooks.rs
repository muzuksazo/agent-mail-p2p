//! Claude Code 훅: MCP 채널이 조직 정책 등으로 꺼져 있어도 메일을 받게 하는 경로.
//!
//! - `post-tool`  (PostToolUse): 일하는 중 도구 결과 옆에 "새 메일" 알림을 끼워 넣는다.
//! - `prompt`     (UserPromptSubmit): 사람이 말할 때 같은 알림을 끼워 넣는다.
//! - `stop-wait`  (Stop, asyncRewake): 답을 마친 뒤 백그라운드로 기다리다 메일이 오면
//!   종료 코드 2로 끝나 쉬고 있던 세션을 깨운다. 같은 세션에서 새 대기가 시작되면 옛 대기는 조용히 끝난다.
//!
//! 알림은 한 번만 간다(데몬의 'sent' → 'picked'). 본문은 MCP `fetch_inbox`로 읽어야 '읽음'이 된다.

use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{json, Value};

use crate::layer5_mcp_bridge::reqwest_free_http_get;

fn take_hints(daemon_url: &str, agent: &str, session: &str, timeout_secs: u64) -> Result<Vec<Value>> {
    let res = reqwest_free_http_get(&format!("{daemon_url}/api/wait?agent={agent}&session={session}&timeout={timeout_secs}"))?;
    Ok(res.get("hints").and_then(|v| v.as_array()).cloned().unwrap_or_default())
}

fn summary(hints: &[Value]) -> String {
    let lines: Vec<String> = hints
        .iter()
        .map(|h| {
            format!(
                "@{}: {}",
                h.get("sender").and_then(|v| v.as_str()).unwrap_or("팀원"),
                h.get("hint").and_then(|v| v.as_str()).unwrap_or("")
            )
        })
        .collect();
    format!(
        "[단톡방 새 메일 {}개] {} — agent-mail의 fetch_inbox 도구로 본문을 읽고, 답이 필요하면 send_mail로 답하세요.",
        hints.len(),
        lines.join(" | ")
    )
}

fn read_stdin_json() -> Value {
    let mut raw = String::new();
    let _ = std::io::stdin().read_to_string(&mut raw);
    serde_json::from_str(&raw).unwrap_or(Value::Null)
}

/// 훅 입력의 session_id를 URL에 넣어도 안전한 글자만 남겨 쓴다.
fn session_key(input: &Value) -> String {
    let raw = input.get("session_id").and_then(|v| v.as_str()).unwrap_or("default");
    let k: String = raw.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-').take(64).collect();
    if k.is_empty() { "default".into() } else { k }
}

fn wait_marker(agent: &str, session: &str) -> PathBuf {
    let safe: String = session.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-').take(64).collect();
    std::env::temp_dir().join(format!("agent-mail-wait-{agent}-{safe}.pid"))
}

pub fn run(mode: &str, agent: &str, daemon_url: &str, max_secs: u64) -> Result<()> {
    match mode {
        "post-tool" | "prompt" => {
            let session = session_key(&read_stdin_json());
            // 데몬이 꺼져 있으면 아무것도 끼워 넣지 않는다(작업을 방해하지 않음).
            let Ok(hints) = take_hints(daemon_url, agent, &session, 0) else { return Ok(()) };
            if !hints.is_empty() {
                let event = if mode == "prompt" { "UserPromptSubmit" } else { "PostToolUse" };
                println!(
                    "{}",
                    json!({ "hookSpecificOutput": { "hookEventName": event, "additionalContext": summary(&hints) } })
                );
            }
            Ok(())
        }
        "stop-wait" => {
            let session = session_key(&read_stdin_json());
            let marker = wait_marker(agent, &session);
            let me = std::process::id().to_string();
            let _ = std::fs::write(&marker, &me);
            // 이 대기를 띄운 Claude 세션이 닫히면 부모가 바뀐다(고아). 그때는 기다릴 이유가 없다.
            #[cfg(unix)]
            let parent_at_start = std::os::unix::process::parent_id();
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(max_secs) {
                #[cfg(unix)]
                if std::os::unix::process::parent_id() != parent_at_start {
                    return Ok(());
                }
                // 같은 세션에서 더 새 대기가 시작됐으면 이 대기는 할 일이 없다.
                if std::fs::read_to_string(&marker).ok().as_deref() != Some(me.as_str()) {
                    return Ok(());
                }
                match take_hints(daemon_url, agent, &session, 25) {
                    Ok(hints) if !hints.is_empty() => {
                        let _ = std::fs::remove_file(&marker);
                        // asyncRewake: 종료 코드 2 + stderr 내용이 Claude에게 전달되며 세션이 깨어난다.
                        eprintln!("{}", summary(&hints));
                        std::process::exit(2);
                    }
                    Ok(_) => {}
                    Err(_) => std::thread::sleep(Duration::from_secs(5)),
                }
            }
            Ok(())
        }
        other => anyhow::bail!("unknown hook mode: {other} (post-tool | prompt | stop-wait)"),
    }
}
