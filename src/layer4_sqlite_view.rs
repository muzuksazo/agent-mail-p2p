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
}

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
            ",
        )?;

        let (event_tx, _) = broadcast::channel(256);
        Ok(Self {
            inner: Mutex::new(InnerState {
                conn,
                staging_headers: HashMap::new(),
                arbiter: DeterministicLeaseArbiter::default(),
                crdt: TeamCrdtDoc::new(peer_id_u64),
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

    pub fn apply_lease_claim(&self, claim: LeaseClaim) -> std::result::Result<(), LeaseClaim> {
        let res = {
            let mut guard = self.inner.lock().unwrap();
            guard.arbiter.apply_claim(claim, now_unix_ms())
        };
        self.wake_notify.notify_waiters();
        res
    }

    pub fn release_lease(&self, holder: &str, glob_pattern: &str) -> bool {
        let ok = {
            let mut guard = self.inner.lock().unwrap();
            guard.arbiter.release(holder, glob_pattern)
        };
        if ok {
            self.wake_notify.notify_waiters();
        }
        ok
    }

    pub fn check_path_conflict(&self, agent_id: &str, path: &str) -> Option<LeaseClaim> {
        let mut guard = self.inner.lock().unwrap();
        guard.arbiter.check_path(agent_id, path, now_unix_ms())
    }

    pub fn merge_loro_delta(&self, delta: &[u8]) -> Result<()> {
        {
            let guard = self.inner.lock().unwrap();
            guard.crdt.import_delta(delta)?;
        }
        self.wake_notify.notify_waiters();
        Ok(())
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
    ) -> Result<Vec<u8>> {
        let delta = {
            let guard = self.inner.lock().unwrap();
            guard
                .crdt
                .upsert_agent_state(agent_id, name, role, engine, task, progress, status)?
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
             WHERE (recipient = ?1 OR recipient = 'ALL') AND sender != ?1
               AND priority = 'urgent' AND pushed_channel = 0 AND status IN ('sent', 'picked')
             ORDER BY hlc_wall_ms ASC"
        } else {
            "SELECT id, sender, recipient, priority, hint, hlc_wall_ms
             FROM messages
             WHERE (recipient = ?1 OR recipient = 'ALL') AND sender != ?1
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
                    "UPDATE messages SET pushed_channel = 1, status = CASE WHEN status = 'sent' THEN 'picked' ELSE status END WHERE id = ?1",
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

    pub fn fetch_inbox_bodies(&self, agent_id: &str) -> Vec<serde_json::Value> {
        let guard = self.inner.lock().unwrap();
        let mut stmt = match guard.conn.prepare(
            "SELECT id, sender, recipient, priority, hint, body_md, status, hlc_wall_ms
             FROM messages
             WHERE (recipient = ?1 OR recipient = 'ALL') AND sender != ?1 AND status IN ('sent', 'picked')
             ORDER BY hlc_wall_ms ASC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let items: Vec<serde_json::Value> = stmt
            .query_map(params![agent_id], |r| {
                Ok(serde_json::json!({
                    "id": r.get::<_, String>(0)?,
                    "sender": r.get::<_, String>(1)?,
                    "recipient": r.get::<_, String>(2)?,
                    "priority": r.get::<_, String>(3)?,
                    "hint": r.get::<_, String>(4)?,
                    "body_md": r.get::<_, String>(5)?,
                    "status": r.get::<_, String>(6)?,
                    "created_ms": r.get::<_, i64>(7)?,
                }))
            })
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|x| x.ok())
            .collect();

        for it in &items {
            if let Some(id) = it.get("id").and_then(|v| v.as_str()) {
                let _ = guard.conn.execute(
                    "UPDATE messages SET status = 'picked' WHERE id = ?1 AND status = 'sent'",
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
        let agents = guard.crdt.list_agents_json();

        let mut msgs = Vec::new();
        if let Ok(mut stmt) = guard.conn.prepare(
            "SELECT id, sender, recipient, priority, hint, body_md, status, ack_note, hlc_wall_ms
             FROM messages ORDER BY hlc_wall_ms ASC LIMIT 80",
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
