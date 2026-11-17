//! Layer 4: Consistent-Visibility Staging Buffer & Local SQLite WAL + FTS5 Projection
//!
//! - Loro 블로그가 지적한 로컬-퍼스트 에이전트 시스템의 최대 함정("알림 힌트만 먼저 도착하고 본문 블롭이
//!   아직 도착하지 않아 에이전트가 빈 인박스를 읽는 현상")을 원천 차단합니다.
//! - `MailHeader`가 도착했을 때 `inline_body`가 없다면 `staging_headers` 대기실에 보관하고
//!   Layer 1의 `SYNC_BLOB_ALPN` 스트림으로 본문을 당겨 옵니다.
//! - 오직 `blake3::hash(body) == header.body_blake3` 검증을 통과한 직후에만
//!   단일 SQLite WAL 트랜잭션으로 `messages` + `messages_fts`에 원자적으로 기록하고,
//!   그 순간에만 `tokio::sync::Notify` / `broadcast` 트리거를 당겨 Layer 5(Channel Push / Hook)를 깨웁니다!

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Notify};

use crate::layer2_reconcile::{
    xor_fingerprint, MailHeader, MailPriority, ReconcileRequest, ReconcileResponse, TimeRange,
};
use crate::layer3_state_auth::{now_unix_ms, DeterministicLeaseArbiter, LeaseClaim, TeamCrdtDoc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedMailEvent {
    pub id: String,
    pub sender: String,
    pub recipient: String,
    pub priority: String,
    pub hint: String,
    pub created_ms: u64,
}

struct InnerState {
    conn: Connection,
    /// 본문 청크 도착을 기다리는 헤더 대기실 (key = body_blake3)
    staging_headers: HashMap<[u8; 32], Vec<MailHeader>>,
    pub arbiter: DeterministicLeaseArbiter,
    pub crdt: TeamCrdtDoc,
    /// 이 노드 자신의 서명 키(hex): 명부에서 '나'는 항상 접속 중으로 보인다.
    local_pk_hex: Option<String>,
}

/// 마지막 신호 후 이 시간 안이면 '접속 중'. 각 노드는 15초마다 신호를 보낸다.
const ONLINE_WINDOW_MS: u64 = 45_000;

pub struct SqliteProjection {
    inner: Mutex<InnerState>,
    /// 새 메일이나 상태 변경이 원자적으로 커밋되었을 때 로컬 롱폴러(`/api/wait`)와 Channel Push 워커를 깨움
    pub wake_notify: Notify,
    /// 로컬 웹 단톡방 SSE 구독자용 실시간 브로드캐스트 채널
    pub event_tx: broadcast::Sender<CommittedMailEvent>,
}

impl SqliteProjection {
    pub fn open(db_path: &Path, peer_id_u64: u64) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 10000;

            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                priority TEXT NOT NULL,
                hint TEXT NOT NULL,
                body_md TEXT NOT NULL,
                body_blake3 BLOB NOT NULL,
                status TEXT NOT NULL DEFAULT 'sent',
                pushed_channel INTEGER NOT NULL DEFAULT 0,
                ack_note TEXT NOT NULL DEFAULT '',
                hlc_wall_ms INTEGER NOT NULL,
                hlc_logical INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_hlc ON messages(hlc_wall_ms, hlc_logical);

            CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                id UNINDEXED,
                sender,
                hint,
                body_md
            );

            -- 파일 리스 영속화: 데몬 재시작 후에도 이 노드가 알던 리스 테이블을 그대로 복원한다.
            -- 세션별 알림 기록: 같은 컴퓨터의 Claude 세션 여러 개가 각자 '새 메일' 알림을 받는다.
            CREATE TABLE IF NOT EXISTS notified (
                session TEXT NOT NULL,
                msg_id TEXT NOT NULL,
                PRIMARY KEY (session, msg_id)
            );

            -- 팀원 명부(Loro 스냅샷) 등 작은 상태 저장소: 재시작해도 꺼진 팀원이 명부에 남는다.
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                val BLOB NOT NULL
            );

            -- 팀원별 마지막 신호 시각(서명 키 기준)
            CREATE TABLE IF NOT EXISTS presence (
                pk_hex TEXT PRIMARY KEY,
                last_seen_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS leases (
                glob_pattern TEXT PRIMARY KEY,
                expires_wall_ms INTEGER NOT NULL,
                claim_json TEXT NOT NULL
            );
            ",
        )?;

        let arbiter = Self::load_leases(&conn)?;
        let crdt = TeamCrdtDoc::new(peer_id_u64);
        let saved_roster: Option<Vec<u8>> = conn
            .query_row("SELECT val FROM kv WHERE key = 'roster'", [], |r| r.get(0))
            .ok();
        if let Some(bytes) = saved_roster {
            if let Err(e) = crdt.import_delta(&bytes) {
                tracing::warn!("Ignoring unreadable saved roster: {e}");
            }
        }

        let (event_tx, _) = broadcast::channel(256);
        Ok(Self {
            inner: Mutex::new(InnerState {
                conn,
                staging_headers: HashMap::new(),
                arbiter,
                crdt,
                local_pk_hex: None,
            }),
            wake_notify: Notify::new(),
            event_tx,
        })
    }

    /// 헤더 수신 처리: 인라인 본문이 있으면 즉시 해시 검증 후 커밋, 없으면 스테이징 대기 후 `false`(Blob 요청 필요) 반환
    pub fn ingest_mail_header(&self, hdr: MailHeader) -> Result<bool> {
        if let Some(body) = hdr.inline_body.clone() {
            let actual = *blake3::hash(body.as_bytes()).as_bytes();
            if actual != hdr.body_blake3 {
                anyhow::bail!("BLAKE3 body digest mismatch on inline gossip payload");
            }
            self.commit_verified_mail(&hdr, &body)?;
            Ok(true)
        } else {
            let mut guard = self.inner.lock().unwrap();
            let already_exists: bool = guard
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM messages WHERE id = ?1)",
                    params![hdr.id],
                    |r| r.get(0),
                )
                .unwrap_or(false);
            if already_exists {
                return Ok(true);
            }
            guard
                .staging_headers
                .entry(hdr.body_blake3)
                .or_default()
                .push(hdr);
            Ok(false)
        }
    }

    /// QUIC `SYNC_BLOB_ALPN`으로 당겨 온 대용량 본문이 도착하면 해시 검증 후 대기 중이던 헤더들을 일괄 커밋!
    pub fn fulfill_staged_blob(&self, expected_hash: [u8; 32], body: &str) -> Result<usize> {
        let actual = *blake3::hash(body.as_bytes()).as_bytes();
        if actual != expected_hash {
            anyhow::bail!("BLAKE3 digest mismatch on fetched blob");
        }
        let staged = {
            let mut guard = self.inner.lock().unwrap();
            guard.staging_headers.remove(&expected_hash).unwrap_or_default()
        };
        let count = staged.len();
        for hdr in staged {
            self.commit_verified_mail(&hdr, body)?;
        }
        Ok(count)
    }

    /// 본문 무결성이 검증된 메시지만 SQLite WAL + FTS5에 원자적으로 커밋하고 로컬 에이전트를 깨움
    pub fn commit_verified_mail(&self, hdr: &MailHeader, body: &str) -> Result<()> {
        let inserted = {
            let mut guard = self.inner.lock().unwrap();
            let tx = guard.conn.transaction()?;
            let changed = tx.execute(
                "
                INSERT OR IGNORE INTO messages
                (id, sender, recipient, priority, hint, body_md, body_blake3, status, pushed_channel, hlc_wall_ms, hlc_logical)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'sent', 0, ?8, ?9)
                ",
                params![
                    hdr.id,
                    hdr.sender_agent,
                    hdr.recipient,
                    hdr.priority.as_str(),
                    hdr.hint,
                    body,
                    hdr.body_blake3.as_slice(),
                    hdr.hlc.wall_ms as i64,
                    hdr.hlc.logical as i64,
                ],
            )?;
            if changed > 0 {
                tx.execute(
                    "INSERT INTO messages_fts (id, sender, hint, body_md) VALUES (?1, ?2, ?3, ?4)",
                    params![hdr.id, hdr.sender_agent, hdr.hint, body],
                )?;
            }
            tx.commit()?;
            changed > 0
        };

        if inserted {
            let ev = CommittedMailEvent {
                id: hdr.id.clone(),
                sender: hdr.sender_agent.clone(),
                recipient: hdr.recipient.clone(),
                priority: hdr.priority.as_str().to_string(),
                hint: hdr.hint.clone(),
                created_ms: hdr.hlc.wall_ms,
            };
            let _ = self.event_tx.send(ev);
            self.wake_notify.notify_waiters();
        }
        Ok(())
    }

    pub fn apply_receipt_update(&self, msg_id: &str, new_status: &str, outcome: &str) -> Result<()> {
        {
            let guard = self.inner.lock().unwrap();
            guard.conn.execute(
                "UPDATE messages SET status = ?1, ack_note = CASE WHEN ?2 != '' THEN ?2 ELSE ack_note END WHERE id = ?3",
                params![new_status, outcome, msg_id],
            )?;
        }
        self.wake_notify.notify_waiters();
        Ok(())
    }

    pub fn apply_withdraw(&self, msg_id: &str) -> Result<bool> {
        let deleted = {
            let guard = self.inner.lock().unwrap();
            let n = guard.conn.execute(
                "DELETE FROM messages WHERE id = ?1 AND status = 'sent'",
                params![msg_id],
            )?;
            if n > 0 {
                let _ = guard
                    .conn
                    .execute("DELETE FROM messages_fts WHERE id = ?1", params![msg_id]);
            }
            n > 0
        };
        if deleted {
            self.wake_notify.notify_waiters();
        }
        Ok(deleted)
    }

    /// 디스크의 리스 테이블을 읽어 중재기를 다시 만든다. 만료된 행은 버리고 지운다.
    /// 복원도 `apply_claim`을 거치므로 겹치는 행이 있으면 실행 중과 같은 규칙으로 승자가 정해진다.
    fn load_leases(conn: &Connection) -> Result<DeterministicLeaseArbiter> {
        let now = now_unix_ms();
        conn.execute("DELETE FROM leases WHERE expires_wall_ms <= ?1", params![now as i64])?;
        let mut arbiter = DeterministicLeaseArbiter::default();
        let mut stmt = conn.prepare("SELECT claim_json FROM leases ORDER BY glob_pattern")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        for row in rows {
            match serde_json::from_str::<LeaseClaim>(&row?) {
                Ok(claim) => {
                    let _ = arbiter.apply_claim(claim, now);
                }
                Err(e) => tracing::warn!("Skipping unreadable persisted lease row: {e}"),
            }
        }
        Ok(arbiter)
    }

    /// 중재기의 현재 리스 집합을 한 트랜잭션으로 디스크에 반영한다(리스는 많아야 수십 개).
    fn persist_leases(state: &mut InnerState) {
        let leases = state.arbiter.snapshot(now_unix_ms());
        let result = (|| -> rusqlite::Result<()> {
            let tx = state.conn.transaction()?;
            tx.execute("DELETE FROM leases", [])?;
            for c in &leases {
                let json = serde_json::to_string(c).unwrap_or_default();
                tx.execute(
                    "INSERT INTO leases (glob_pattern, expires_wall_ms, claim_json) VALUES (?1, ?2, ?3)",
                    params![c.glob_pattern, c.expires_wall_ms as i64, json],
                )?;
            }
            tx.commit()
        })();
        if let Err(e) = result {
            tracing::warn!("Failed to persist lease table: {e}");
        }
    }

    pub fn apply_lease_claim(&self, claim: LeaseClaim) -> std::result::Result<(), LeaseClaim> {
        let res = {
            let mut guard = self.inner.lock().unwrap();
            let res = guard.arbiter.apply_claim(claim, now_unix_ms());
            if res.is_ok() {
                Self::persist_leases(&mut guard);
            }
            res
        };
        self.wake_notify.notify_waiters();
        res
    }

    pub fn release_lease(&self, holder: &str, glob_pattern: &str) -> bool {
        let ok = {
            let mut guard = self.inner.lock().unwrap();
            let ok = guard.arbiter.release(holder, glob_pattern);
            if ok {
                Self::persist_leases(&mut guard);
            }
            ok
        };
        if ok {
            self.wake_notify.notify_waiters();
        }
        ok
    }

    /// 이 에이전트가 쥐고 있는 유효한 리스 목록(주기적 재공지용).
    pub fn leases_held_by(&self, agent_id: &str) -> Vec<LeaseClaim> {
        let mut guard = self.inner.lock().unwrap();
        guard
            .arbiter
            .snapshot(now_unix_ms())
            .into_iter()
            .filter(|c| c.holder_agent == agent_id)
            .collect()
    }

    pub fn check_path_conflict(&self, agent_id: &str, path: &str) -> Option<LeaseClaim> {
        let mut guard = self.inner.lock().unwrap();
        guard.arbiter.check_path(agent_id, path, now_unix_ms())
    }

    /// 팀 보드(Loro) 전체 스냅샷. 주기적으로 다시 알려 늦게 들어온 팀원도 같은 보드로 수렴하게 한다.
    pub fn export_loro_snapshot(&self) -> Result<Vec<u8>> {
        let guard = self.inner.lock().unwrap();
        guard.crdt.export_snapshot()
    }

    fn save_roster(state: &InnerState) {
        if let Ok(snapshot) = state.crdt.export_snapshot() {
            let _ = state.conn.execute(
                "INSERT INTO kv (key, val) VALUES ('roster', ?1) ON CONFLICT(key) DO UPDATE SET val = excluded.val",
                params![snapshot],
            );
        }
    }

    pub fn merge_loro_delta(&self, delta: &[u8]) -> Result<()> {
        {
            let guard = self.inner.lock().unwrap();
            guard.crdt.import_delta(delta)?;
            Self::save_roster(&guard);
        }
        self.wake_notify.notify_waiters();
        Ok(())
    }

    /// 에이전트가 MCP로 정한 '하는 일'·'대화 상대'를 명부에 반영(변경분은 호출한 쪽이 가십으로 퍼뜨린다).
    pub fn update_local_fields(&self, agent_id: &str, task: Option<&str>, target: Option<&str>) -> Result<Vec<u8>> {
        let delta = {
            let guard = self.inner.lock().unwrap();
            let d = guard.crdt.set_agent_fields(agent_id, task, target, now_unix_ms())?;
            Self::save_roster(&guard);
            d
        };
        self.wake_notify.notify_waiters();
        Ok(delta)
    }

    /// 서명 키가 pk인 팀원에게서 신호를 받았다(가십 봉투 검증 직후 호출).
    pub fn touch_presence(&self, pk: &[u8; 32], now_ms: u64) {
        let guard = self.inner.lock().unwrap();
        let _ = guard.conn.execute(
            "INSERT INTO presence (pk_hex, last_seen_ms) VALUES (?1, ?2)
             ON CONFLICT(pk_hex) DO UPDATE SET last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms)",
            params![data_encoding::HEXLOWER.encode(pk), now_ms as i64],
        );
    }

    /// 세션별 '새 메일' 알림: 이 세션에 아직 알리지 않았고 아직 읽지 않은 나에게 온 메일.
    pub fn drain_session_hints(&self, agent_id: &str, session: &str) -> Vec<CommittedMailEvent> {
        self.drain_session_hints_opt(agent_id, session, false)
    }

    /// fresh_only=true: 아직 어느 세션에도 알리지 않은 메일만(세션 시작 인사가 같은 메일로 반복되지 않게).
    pub fn drain_session_hints_opt(&self, agent_id: &str, session: &str, fresh_only: bool) -> Vec<CommittedMailEvent> {
        let guard = self.inner.lock().unwrap();
        let sql = if fresh_only {
            "SELECT id, sender, recipient, priority, hint, hlc_wall_ms FROM messages m
             WHERE (recipient = 'ALL' OR (',' || replace(recipient, ' ', '') || ',') LIKE ('%,' || ?1 || ',%')) AND sender != ?1 AND status IN ('sent', 'picked')
               AND NOT EXISTS (SELECT 1 FROM notified n WHERE n.msg_id = m.id)
               AND ?2 = ?2
             ORDER BY hlc_wall_ms ASC, hlc_logical ASC"
        } else {
            "SELECT id, sender, recipient, priority, hint, hlc_wall_ms FROM messages m
             WHERE (recipient = 'ALL' OR (',' || replace(recipient, ' ', '') || ',') LIKE ('%,' || ?1 || ',%')) AND sender != ?1 AND status IN ('sent', 'picked')
               AND NOT EXISTS (SELECT 1 FROM notified n WHERE n.session = ?2 AND n.msg_id = m.id)
             ORDER BY hlc_wall_ms ASC, hlc_logical ASC"
        };
        let rows: Vec<CommittedMailEvent> = match guard.conn.prepare(sql) {
            Ok(mut stmt) => stmt
                .query_map(params![agent_id, session], |r| {
                    Ok(CommittedMailEvent {
                        id: r.get(0)?,
                        sender: r.get(1)?,
                        recipient: r.get(2)?,
                        priority: r.get(3)?,
                        hint: r.get(4)?,
                        created_ms: r.get::<_, i64>(5)? as u64,
                    })
                })
                .map(|it| it.filter_map(|x| x.ok()).collect())
                .unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        for item in &rows {
            let _ = guard.conn.execute(
                "INSERT OR IGNORE INTO notified (session, msg_id) VALUES (?1, ?2)",
                params![session, item.id],
            );
            let _ = guard.conn.execute(
                "UPDATE messages SET status = 'picked' WHERE id = ?1 AND status = 'sent'",
                params![item.id],
            );
        }
        rows
    }

    pub fn upsert_local_agent(
        &self,
        agent_id: &str,
        name: &str,
        role: &str,
        engine: &str,
        task: &str,
        progress: i64,
        status: &str,
        pk_hex: &str,
    ) -> Result<Vec<u8>> {
        let delta = {
            let mut guard = self.inner.lock().unwrap();
            guard.local_pk_hex = Some(pk_hex.to_string());
            let delta = guard
                .crdt
                .upsert_agent_state(agent_id, name, role, engine, task, progress, status, pk_hex)?;
            Self::save_roster(&guard);
            delta
        };
        self.wake_notify.notify_waiters();
        Ok(delta)
    }

    /// Track 1 (`urgent_only=true` -> Channel Push) 및 Track 2 (`urgent_only=false` -> `/api/wait` & `PostToolUse`) 공용 픽업
    pub fn drain_pending_hints(&self, agent_id: &str, urgent_only: bool) -> Vec<CommittedMailEvent> {
        let guard = self.inner.lock().unwrap();
        let sql = if urgent_only {
            "SELECT id, sender, recipient, priority, hint, hlc_wall_ms
             FROM messages
             WHERE (recipient = 'ALL' OR (',' || replace(recipient, ' ', '') || ',') LIKE ('%,' || ?1 || ',%')) AND sender != ?1
               AND (priority = 'urgent' OR (',' || replace(recipient, ' ', '') || ',') LIKE ('%,' || ?1 || ',%')) AND pushed_channel = 0 AND status IN ('sent', 'picked')
             ORDER BY hlc_wall_ms ASC"
        } else {
            "SELECT id, sender, recipient, priority, hint, hlc_wall_ms
             FROM messages
             WHERE (recipient = 'ALL' OR (',' || replace(recipient, ' ', '') || ',') LIKE ('%,' || ?1 || ',%')) AND sender != ?1
               AND status = 'sent'
             ORDER BY CASE WHEN priority = 'urgent' THEN 0 ELSE 1 END, hlc_wall_ms ASC"
        };

        let mut stmt = match guard.conn.prepare(sql) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows: Vec<CommittedMailEvent> = stmt
            .query_map(params![agent_id], |r| {
                Ok(CommittedMailEvent {
                    id: r.get(0)?,
                    sender: r.get(1)?,
                    recipient: r.get(2)?,
                    priority: r.get(3)?,
                    hint: r.get(4)?,
                    created_ms: r.get::<_, i64>(5)? as u64,
                })
            })
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|x| x.ok())
            .collect();

        for item in &rows {
            if urgent_only {
                let _ = guard.conn.execute(
                    // 채널 푸시는 자기 표시만 바꾼다. 채널이 꺼져 있으면 푸시는 버려지므로 훅 알림('sent'→'picked')을 막으면 안 된다.
                    "UPDATE messages SET pushed_channel = 1 WHERE id = ?1",
                    params![item.id],
                );
            } else {
                let _ = guard.conn.execute(
                    "UPDATE messages SET status = 'picked' WHERE id = ?1 AND status = 'sent'",
                    params![item.id],
                );
            }
        }
        rows
    }

    /// 아직 읽지 않은(보냄·알림 감) 나에게 온 메일 수. 훅 알림이나 채널 푸시만 간 메일도 포함한다.
    pub fn unread_count(&self, agent_id: &str) -> i64 {
        let guard = self.inner.lock().unwrap();
        guard
            .conn
            .query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE (recipient = 'ALL' OR (',' || replace(recipient, ' ', '') || ',') LIKE ('%,' || ?1 || ',%')) AND sender != ?1 AND status IN ('sent', 'picked')",
                params![agent_id],
                |r| r.get(0),
            )
            .unwrap_or(0)
    }

    /// 안 읽은 메일 본문을 시간순으로 돌려주고 '읽음'으로 표시한다(fetch_inbox / wait_mail 도구).
    pub fn read_inbox(&self, agent_id: &str) -> Vec<serde_json::Value> {
        let guard = self.inner.lock().unwrap();
        let mut stmt = match guard.conn.prepare(
            "SELECT id, sender, recipient, priority, body_md, hlc_wall_ms
             FROM messages
             WHERE (recipient = 'ALL' OR (',' || replace(recipient, ' ', '') || ',') LIKE ('%,' || ?1 || ',%')) AND sender != ?1 AND status IN ('sent', 'picked')
             ORDER BY hlc_wall_ms ASC, hlc_logical ASC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let items: Vec<serde_json::Value> = stmt
            .query_map(params![agent_id], |r| {
                Ok(serde_json::json!({
                    "id": r.get::<_, String>(0)?,
                    "from": r.get::<_, String>(1)?,
                    "to": r.get::<_, String>(2)?,
                    "priority": r.get::<_, String>(3)?,
                    "body": r.get::<_, String>(4)?,
                    "sent_at_ms": r.get::<_, i64>(5)?,
                }))
            })
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|x| x.ok())
            .collect();
        drop(stmt);
        for it in &items {
            if let Some(id) = it.get("id").and_then(|v| v.as_str()) {
                let _ = guard.conn.execute(
                    "UPDATE messages SET status = 'read', pushed_channel = 1 WHERE id = ?1",
                    params![id],
                );
            }
        }
        items
    }

    pub fn full_state_json(&self) -> serde_json::Value {
        let mut guard = self.inner.lock().unwrap();
        let now = now_unix_ms();
        let leases = guard.arbiter.snapshot(now);
        let mut agents = guard.crdt.list_agents_json();
        for a in agents.iter_mut() {
            let pk = a.get("pk").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let is_me = !pk.is_empty() && guard.local_pk_hex.as_deref() == Some(pk.as_str());
            let seen: Option<i64> = if pk.is_empty() {
                None
            } else {
                guard.conn.query_row("SELECT last_seen_ms FROM presence WHERE pk_hex = ?1", params![pk], |r| r.get(0)).ok()
            };
            let last_seen = if is_me { Some(now as i64) } else { seen };
            a["last_seen_ms"] = serde_json::json!(last_seen);
            a["online"] = serde_json::json!(last_seen.map(|t| now.saturating_sub(t as u64) <= ONLINE_WINDOW_MS).unwrap_or(false));
            if let Some(obj) = a.as_object_mut() {
                obj.remove("pk");
            }
        }

        let mut msgs = Vec::new();
        if let Ok(mut stmt) = guard.conn.prepare(
            // 가장 최근 80개를 시간순으로 (예전에는 가장 오래된 80개만 보여 새 메시지가 화면에서 사라졌다)
            "SELECT * FROM (
               SELECT id, sender, recipient, priority, hint, body_md, status, ack_note, hlc_wall_ms, hlc_logical
               FROM messages ORDER BY hlc_wall_ms DESC, hlc_logical DESC LIMIT 80
             ) ORDER BY hlc_wall_ms ASC, hlc_logical ASC",
        ) {
            if let Ok(iter) = stmt.query_map([], |r| {
                Ok(serde_json::json!({
                    "id": r.get::<_, String>(0)?,
                    "sender": r.get::<_, String>(1)?,
                    "recipient": r.get::<_, String>(2)?,
                    "priority": r.get::<_, String>(3)?,
                    "hint": r.get::<_, String>(4)?,
                    "body_md": r.get::<_, String>(5)?,
                    "status": r.get::<_, String>(6)?,
                    "ack_note": r.get::<_, String>(7)?,
                    "created_at": (r.get::<_, i64>(8)? as f64) / 1000.0,
                }))
            }) {
                for row in iter.flatten() {
                    msgs.push(row);
                }
            }
        }

        serde_json::json!({
            "mission_title": "5인 P2P 로컬-퍼스트 팀플 스웜",
            "agents": agents,
            "leases": leases,
            "messages": msgs,
        })
    }

    /// Willow 스타일 시간 구간 `[start_ms, end_ms)` 지문 계산 및 이진 분할 응답
    pub fn range_fingerprint(&self, range: TimeRange) -> ([u8; 32], u32) {
        let guard = self.inner.lock().unwrap();
        let mut stmt = match guard.conn.prepare(
            "SELECT id, body_blake3 FROM messages WHERE hlc_wall_ms >= ?1 AND hlc_wall_ms < ?2",
        ) {
            Ok(s) => s,
            Err(_) => return ([0u8; 32], 0),
        };
        let pairs: Vec<(String, [u8; 32])> = stmt
            .query_map(params![range.start_ms as i64, range.end_ms as i64], |r| {
                let id: String = r.get(0)?;
                let blob: Vec<u8> = r.get(1)?;
                let mut arr = [0u8; 32];
                if blob.len() == 32 {
                    arr.copy_from_slice(&blob);
                }
                Ok((id, arr))
            })
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|x| x.ok())
            .collect();

        let fp = xor_fingerprint(pairs.iter().map(|(id, h)| (id.as_str(), h)));
        (fp, pairs.len() as u32)
    }

    pub fn handle_reconcile_request(&self, req: ReconcileRequest) -> ReconcileResponse {
        match req {
            ReconcileRequest::CompareRange {
                range,
                caller_fingerprint,
            } => {
                let (local_fp, _cnt) = self.range_fingerprint(range);
                if local_fp == caller_fingerprint {
                    return ReconcileResponse::RangeMatched;
                }
                let mid = range.start_ms + (range.end_ms.saturating_sub(range.start_ms)) / 2;
                let left_r = TimeRange {
                    start_ms: range.start_ms,
                    end_ms: mid.max(range.start_ms + 1),
                };
                let right_r = TimeRange {
                    start_ms: mid.max(range.start_ms + 1),
                    end_ms: range.end_ms.max(mid + 1),
                };
                let (l_fp, l_cnt) = self.range_fingerprint(left_r);
                let (r_fp, r_cnt) = self.range_fingerprint(right_r);
                ReconcileResponse::RangeMismatch {
                    left_half: (left_r, l_fp, l_cnt),
                    right_half: (right_r, r_fp, r_cnt),
                }
            }
            ReconcileRequest::FetchRangeEntries { range, known_ids } => {
                let known: HashSet<String> = known_ids.into_iter().collect();
                let guard = self.inner.lock().unwrap();
                let mut entries = Vec::new();
                if let Ok(mut stmt) = guard.conn.prepare(
                    "SELECT id, sender, recipient, priority, hint, body_md, body_blake3, hlc_wall_ms, hlc_logical
                     FROM messages WHERE hlc_wall_ms >= ?1 AND hlc_wall_ms < ?2",
                ) {
                    if let Ok(iter) =
                        stmt.query_map(params![range.start_ms as i64, range.end_ms as i64], |r| {
                            let id: String = r.get(0)?;
                            let sender_agent: String = r.get(1)?;
                            let recipient: String = r.get(2)?;
                            let prio_s: String = r.get(3)?;
                            let hint: String = r.get(4)?;
                            let body_md: String = r.get(5)?;
                            let b3: Vec<u8> = r.get(6)?;
                            let wall_ms = r.get::<_, i64>(7)? as u64;
                            let logical = r.get::<_, i64>(8)? as u32;
                            let mut body_blake3 = [0u8; 32];
                            if b3.len() == 32 {
                                body_blake3.copy_from_slice(&b3);
                            }
                            Ok((
                                MailHeader {
                                    id,
                                    sender_agent,
                                    recipient,
                                    priority: if prio_s == "urgent" {
                                        MailPriority::Urgent
                                    } else {
                                        MailPriority::Normal
                                    },
                                    hint,
                                    body_blake3,
                                    body_len: body_md.len() as u32,
                                    inline_body: None,
                                    hlc: crate::layer3_state_auth::HlcTimestamp {
                                        wall_ms,
                                        logical,
                                        node_tag: [0u8; 8],
                                    },
                                },
                                body_md,
                            ))
                        })
                    {
                        for pair in iter.flatten() {
                            if !known.contains(&pair.0.id) {
                                entries.push(pair);
                            }
                        }
                    }
                }
                let loro_snapshot = guard.crdt.export_snapshot().unwrap_or_default();
                ReconcileResponse::MissingEntries {
                    entries,
                    loro_snapshot,
                }
            }
            ReconcileRequest::FetchBlob { blake3_hash } => {
                let guard = self.inner.lock().unwrap();
                let found: Option<String> = guard
                    .conn
                    .query_row(
                        "SELECT body_md FROM messages WHERE body_blake3 = ?1 LIMIT 1",
                        params![blake3_hash.as_slice()],
                        |r| r.get(0),
                    )
                    .ok();
                ReconcileResponse::BlobPayload {
                    found: found.is_some(),
                    content: found,
                }
            }
        }
    }
}

#[cfg(test)]
mod lease_persistence_tests {
    use super::*;
    use crate::layer3_state_auth::HlcTimestamp;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "amp2p-test-{}-{}-{}",
            name,
            std::process::id(),
            now_unix_ms()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("mail.sqlite3")
    }

    fn claim(pattern: &str, agent: &str, expires_wall_ms: u64) -> LeaseClaim {
        LeaseClaim {
            glob_pattern: pattern.to_string(),
            holder_agent: agent.to_string(),
            holder_pk: [7u8; 32],
            acquired_hlc: HlcTimestamp { wall_ms: now_unix_ms(), logical: 0, node_tag: [7u8; 8] },
            expires_wall_ms,
            reason: String::new(),
            overseer_override: false,
        }
    }

    fn patterns(store: &SqliteProjection) -> Vec<String> {
        let mut guard = store.inner.lock().unwrap();
        let mut v: Vec<String> = guard
            .arbiter
            .snapshot(now_unix_ms())
            .into_iter()
            .map(|c| c.glob_pattern)
            .collect();
        v.sort();
        v
    }

    #[test]
    fn lease_survives_restart() {
        let db = temp_db("survive");
        {
            let store = SqliteProjection::open(&db, 1).unwrap();
            store.apply_lease_claim(claim("src/api/*", "be", now_unix_ms() + 60_000)).unwrap();
        }
        let store = SqliteProjection::open(&db, 1).unwrap();
        assert_eq!(patterns(&store), vec!["src/api/*".to_string()]);
        // 재시작 후에도 다른 에이전트의 편집은 충돌로 잡혀야 한다
        assert!(store.check_path_conflict("fe", "src/api/user.rs").is_some());
    }

    #[test]
    fn released_lease_stays_released_after_restart() {
        let db = temp_db("release");
        {
            let store = SqliteProjection::open(&db, 1).unwrap();
            store.apply_lease_claim(claim("src/api/*", "be", now_unix_ms() + 60_000)).unwrap();
            assert!(store.release_lease("be", "src/api/*"));
        }
        let store = SqliteProjection::open(&db, 1).unwrap();
        assert!(patterns(&store).is_empty());
    }

    #[test]
    fn expired_lease_is_not_restored() {
        let db = temp_db("expired");
        {
            let store = SqliteProjection::open(&db, 1).unwrap();
            store.apply_lease_claim(claim("src/ui/*", "fe", now_unix_ms() + 150)).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        let store = SqliteProjection::open(&db, 1).unwrap();
        assert!(patterns(&store).is_empty());
    }

    #[test]
    fn losing_lease_is_removed_from_disk_too() {
        let db = temp_db("loser");
        {
            let store = SqliteProjection::open(&db, 1).unwrap();
            let mut late = claim("src/db/*", "db", now_unix_ms() + 60_000);
            late.acquired_hlc.wall_ms += 1_000;
            store.apply_lease_claim(late).unwrap();
            let mut early = claim("src/db/*", "be", now_unix_ms() + 60_000);
            early.acquired_hlc.wall_ms -= 1_000;
            store.apply_lease_claim(early).unwrap(); // 더 이른 HLC가 이긴다
        }
        let store = SqliteProjection::open(&db, 1).unwrap();
        let mut guard = store.inner.lock().unwrap();
        let holders: Vec<String> = guard.arbiter.snapshot(now_unix_ms()).into_iter().map(|c| c.holder_agent).collect();
        assert_eq!(holders, vec!["be".to_string()]);
    }
}

#[cfg(test)]
mod inbox_tests {
    use super::*;
    use crate::layer2_reconcile::{MailHeader, MailPriority};
    use crate::layer3_state_auth::HlcTimestamp;

    fn store(name: &str) -> SqliteProjection {
        let dir = std::env::temp_dir().join(format!("amp2p-inbox-{}-{}-{}", name, std::process::id(), now_unix_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        SqliteProjection::open(&dir.join("m.sqlite3"), 1).unwrap()
    }

    fn put(s: &SqliteProjection, id: &str, from: &str, to: &str, urgent: bool, body: &str) {
        let hdr = MailHeader {
            id: id.into(),
            sender_agent: from.into(),
            recipient: to.into(),
            priority: if urgent { MailPriority::Urgent } else { MailPriority::Normal },
            hint: body.into(),
            body_blake3: *blake3::hash(body.as_bytes()).as_bytes(),
            body_len: body.len() as u32,
            inline_body: Some(body.into()),
            hlc: HlcTimestamp { wall_ms: now_unix_ms(), logical: 0, node_tag: [0; 8] },
        };
        s.commit_verified_mail(&hdr, body).unwrap();
    }

    fn ids(v: &[serde_json::Value]) -> Vec<String> {
        v.iter().map(|m| m["id"].as_str().unwrap().to_string()).collect()
    }

    #[test]
    fn inbox_returns_mine_and_all_then_marks_read() {
        let s = store("read");
        put(&s, "a", "pm", "be", false, "to be");
        put(&s, "b", "pm", "ALL", false, "to all");
        put(&s, "c", "pm", "qa", false, "to qa");
        put(&s, "d", "be", "ALL", false, "from me");
        assert_eq!(ids(&s.read_inbox("be")), vec!["a", "b"]);
        assert!(s.read_inbox("be").is_empty(), "읽은 메일은 다시 나오지 않아야 한다");
        assert_eq!(s.unread_count("be"), 0);
    }

    #[test]
    fn hinted_mail_is_still_unread_until_fetched() {
        let s = store("hinted");
        put(&s, "a", "pm", "be", false, "hello");
        assert_eq!(s.drain_pending_hints("be", false).len(), 1, "훅 알림이 한 번 나간다");
        assert!(s.drain_pending_hints("be", false).is_empty(), "같은 알림은 반복되지 않는다");
        assert_eq!(s.unread_count("be"), 1, "알림만 간 메일은 아직 안 읽음");
        assert_eq!(ids(&s.read_inbox("be")), vec!["a"]);
    }

    #[test]
    fn channel_push_does_not_steal_hook_notice() {
        // 채널이 정책으로 꺼져 있으면 푸시는 버려진다. 그래도 훅 알림은 따로 한 번 가야 한다.
        let s = store("both");
        put(&s, "a", "pm", "be", false, "direct");
        assert_eq!(s.drain_pending_hints("be", true).len(), 1, "채널 푸시");
        assert_eq!(s.drain_pending_hints("be", false).len(), 1, "훅 알림도 받아야 한다");
    }

    #[test]
    fn push_covers_urgent_and_direct_but_not_broadcast_chatter() {
        let s = store("push");
        put(&s, "u", "pm", "ALL", true, "urgent to all");
        put(&s, "d", "pm", "be", false, "direct normal");
        put(&s, "n", "pm", "ALL", false, "chatter");
        let pushed: Vec<String> = s.drain_pending_hints("be", true).into_iter().map(|e| e.id).collect();
        assert_eq!(pushed, vec!["u", "d"]);
    }
}

#[cfg(test)]
mod session_and_roster_tests {
    use super::*;
    use crate::layer2_reconcile::{MailHeader, MailPriority};
    use crate::layer3_state_auth::HlcTimestamp;

    fn db(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("amp2p-sr-{}-{}-{}", name, std::process::id(), now_unix_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("m.sqlite3")
    }

    fn put(s: &SqliteProjection, id: &str, to: &str) {
        let body = format!("body {id}");
        let hdr = MailHeader {
            id: id.into(), sender_agent: "pm".into(), recipient: to.into(), priority: MailPriority::Normal,
            hint: body.clone(), body_blake3: *blake3::hash(body.as_bytes()).as_bytes(), body_len: body.len() as u32,
            inline_body: Some(body.clone()), hlc: HlcTimestamp { wall_ms: now_unix_ms(), logical: 0, node_tag: [0; 8] },
        };
        s.commit_verified_mail(&hdr, &body).unwrap();
    }

    fn ids(v: Vec<CommittedMailEvent>) -> Vec<String> {
        v.into_iter().map(|e| e.id).collect()
    }

    #[test]
    fn every_session_gets_its_own_notice() {
        let s = SqliteProjection::open(&db("sess"), 1).unwrap();
        put(&s, "a", "be");
        assert_eq!(ids(s.drain_session_hints("be", "s1")), vec!["a"]);
        assert_eq!(ids(s.drain_session_hints("be", "s2")), vec!["a"], "두 번째 세션도 알림을 받는다");
        assert!(s.drain_session_hints("be", "s1").is_empty(), "같은 세션에는 한 번만");
    }

    #[test]
    fn session_start_greets_only_for_mail_no_session_was_told() {
        let s = SqliteProjection::open(&db("fresh"), 1).unwrap();
        put(&s, "old", "be");
        assert_eq!(ids(s.drain_session_hints("be", "s1")), vec!["old"], "기존 세션이 이미 들음");
        put(&s, "new", "be");
        assert_eq!(ids(s.drain_session_hints_opt("be", "s2", true)), vec!["new"], "새 세션 인사는 처음 듣는 메일만");
        assert!(s.drain_session_hints_opt("be", "s3", true).is_empty(), "다음 세션은 조용히");
        assert_eq!(ids(s.drain_session_hints("be", "s3")), vec!["old", "new"], "말을 걸면 안 읽은 메일은 다 알림");
    }

    #[test]
    fn only_mentioned_members_are_notified() {
        let s = SqliteProjection::open(&db("mention"), 1).unwrap();
        put(&s, "m", "be,qa");
        assert_eq!(ids(s.drain_session_hints("be", "s")), vec!["m"], "be는 멘션됨");
        assert_eq!(ids(s.drain_session_hints("qa", "s-qa")), vec!["m"], "qa도 멘션됨");
        assert!(s.drain_session_hints("fe", "s-fe").is_empty(), "fe는 멘션 안 됨");
        assert!(s.drain_session_hints("b", "s-b").is_empty(), "아이디 일부만 같아도 안 됨");
        assert_eq!(s.unread_count("qa"), 1);
        assert_eq!(s.unread_count("fe"), 0);
    }

    #[test]
    fn read_mail_is_not_announced_to_new_sessions() {
        let s = SqliteProjection::open(&db("readsess"), 1).unwrap();
        put(&s, "a", "be");
        assert_eq!(s.read_inbox("be").len(), 1);
        assert!(s.drain_session_hints("be", "late").is_empty());
    }

    #[test]
    fn roster_survives_restart() {
        let path = db("roster");
        {
            let s = SqliteProjection::open(&path, 7).unwrap();
            s.upsert_local_agent("be", "도윤", "팀원", "claude-code", "", 0, "working", "aa").unwrap();
        }
        let s = SqliteProjection::open(&path, 7).unwrap();
        let st = s.full_state_json();
        let names: Vec<&str> = st["agents"].as_array().unwrap().iter().map(|a| a["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["도윤"]);
    }

    #[test]
    fn agent_sets_status_and_target_and_they_survive_restart() {
        let path = db("status");
        {
            let s = SqliteProjection::open(&path, 9).unwrap();
            s.upsert_local_agent("pm", "윤여원", "팀장", "claude-code", "P2P 스웜 협업 중", 10, "working", "ab").unwrap();
            s.update_local_fields("pm", Some("README 합치는 중"), Some("yyw")).unwrap();
            let a = &s.full_state_json()["agents"][0];
            assert_eq!(a["task"], "README 합치는 중");
            assert_eq!(a["target"], "yyw");
        }
        // 데몬 재시작: 시작할 때 기본 문구로 다시 등록해도 에이전트가 정한 값은 남는다
        let s = SqliteProjection::open(&path, 9).unwrap();
        s.upsert_local_agent("pm", "윤여원", "팀장", "claude-code", "P2P 스웜 협업 중", 10, "working", "ab").unwrap();
        let a = &s.full_state_json()["agents"][0];
        assert_eq!(a["task"], "README 합치는 중");
        assert_eq!(a["target"], "yyw");
    }

    #[test]
    fn presence_follows_last_signal() {
        // 다른 컴퓨터(노드 2)의 팀원 '지호'가 명부로 들어온 상황
        let remote = SqliteProjection::open(&db("presence-remote"), 2).unwrap();
        let delta = remote.upsert_local_agent("qa", "지호", "팀원", "claude-code", "", 0, "working", &"11".repeat(32)).unwrap();
        let s = SqliteProjection::open(&db("presence"), 1).unwrap();
        s.merge_loro_delta(&delta).unwrap();
        let pk = [0x11u8; 32];
        s.touch_presence(&pk, now_unix_ms() - 10 * 60_000);
        let a = &s.full_state_json()["agents"][0];
        assert_eq!(a["online"], false, "10분 전 신호면 꺼짐");
        s.touch_presence(&pk, now_unix_ms());
        let a = &s.full_state_json()["agents"][0];
        assert_eq!(a["online"], true, "방금 신호면 접속 중");
        assert!(a["last_seen_ms"].as_u64().unwrap() > 0);
    }
}
