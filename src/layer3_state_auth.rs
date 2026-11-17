//! Layer 3: Hybrid Logical Clock (HLC), Meadowcap Capability Auth, Loro CRDT & Lease Arbiter
//!
//! 1. `HlcTimestamp`: 물리 시계 오차와 무관하게 전체 피어 간 완전 순서(Total Order)를 부여합니다.
//! 2. `CapabilityCert`: 중앙 인증 서버 없이 팀장 루트 키(`overseer_root_pk`) 서명만으로
//!    `OVERSEER` 명령 사칭과 허가되지 않은 파일 글롭 침범을 암호학적으로 차단합니다.
//! 3. `LeaseArbiter`: 단순 CRDT로 풀 수 없는 배타적 파일 리스를 `(HlcTimestamp, holder_pk)`
//!    결정론적 선점 규칙과 짧은 하트비트 TTL로 중재해 5대 PC가 항상 같은 잠금 상태로 수렴합니다.
//! 4. `TeamCrdtDoc`: `loro::LoroDoc`을 사용해 에이전트 로스터·진척도·결재 카드를 무충돌 병합합니다.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use glob_match::glob_match;
use loro::{ExportMode, LoroDoc, LoroValue};
use serde::{Deserialize, Serialize};

use crate::layer2_reconcile::GossipKind;

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 하이브리드 논리 시계 (Wall-clock ms + 논리 카운터 + 피어 공개키 앞 8바이트 타이브레이커)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HlcTimestamp {
    pub wall_ms: u64,
    pub logical: u32,
    pub node_tag: [u8; 8],
}

pub struct HybridClock {
    last_wall_ms: u64,
    logical: u32,
    node_tag: [u8; 8],
}

impl HybridClock {
    pub fn new(node_pk: &[u8; 32]) -> Self {
        let mut node_tag = [0u8; 8];
        node_tag.copy_from_slice(&node_pk[..8]);
        Self {
            last_wall_ms: 0,
            logical: 0,
            node_tag,
        }
    }

    pub fn tick(&mut self) -> HlcTimestamp {
        let phys = now_unix_ms();
        if phys > self.last_wall_ms {
            self.last_wall_ms = phys;
            self.logical = 0;
        } else {
            self.logical = self.logical.saturating_add(1);
        }
        HlcTimestamp {
            wall_ms: self.last_wall_ms,
            logical: self.logical,
            node_tag: self.node_tag,
        }
    }

    pub fn observe(&mut self, remote: &HlcTimestamp) -> HlcTimestamp {
        let phys = now_unix_ms();
        let max_wall = phys.max(self.last_wall_ms).max(remote.wall_ms);
        if max_wall == self.last_wall_ms && max_wall == remote.wall_ms {
            self.logical = self.logical.max(remote.logical).saturating_add(1);
        } else if max_wall == self.last_wall_ms {
            self.logical = self.logical.saturating_add(1);
        } else if max_wall == remote.wall_ms {
            self.logical = remote.logical.saturating_add(1);
        } else {
            self.logical = 0;
        }
        self.last_wall_ms = max_wall;
        HlcTimestamp {
            wall_ms: self.last_wall_ms,
            logical: self.logical,
            node_tag: self.node_tag,
        }
    }
}

/// Meadowcap 영감을 받은 역할 및 경로 범위 권한 증명서
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoleScope {
    /// 팀장 루트 권한: OVERSEER 발신, 결재 확정, 강제 리스 선점 가능
    Overseer,
    /// 팀원 에이전트 권한: 지정된 `agent_id`와 허용된 경로 프리픽스 내에서만 활동 가능
    Agent {
        agent_id: String,
        allowed_glob_prefix: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityCert {
    pub delegate_pk: [u8; 32],
    pub scope: RoleScope,
    pub expires_ms: u64,
    pub issuer_sig: Vec<u8>,
}

impl CapabilityCert {
    pub fn issue(
        overseer_root_sk: &SigningKey,
        delegate_pk: [u8; 32],
        scope: RoleScope,
        ttl_ms: u64,
    ) -> Result<Self> {
        let expires_ms = now_unix_ms().saturating_add(ttl_ms);
        let bytes = postcard::to_stdvec(&(&delegate_pk, &scope, expires_ms))?;
        let sig: Signature = overseer_root_sk.sign(&bytes);
        Ok(Self {
            delegate_pk,
            scope,
            expires_ms,
            issuer_sig: sig.to_bytes().to_vec(),
        })
    }

    pub fn verify(&self, overseer_root_pk: &[u8; 32], author_pk: &[u8; 32], now_ms: u64) -> Result<()> {
        if &self.delegate_pk != author_pk {
            bail!("Capability delegate_pk mismatch (token theft attempt blocked)");
        }
        if now_ms > self.expires_ms {
            bail!("Capability certificate expired");
        }
        let root_vk = VerifyingKey::from_bytes(overseer_root_pk)?;
        let sig_arr: [u8; 64] = self
            .issuer_sig
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid capability signature length"))?;
        let sig = Signature::from_bytes(&sig_arr);
        let bytes = postcard::to_stdvec(&(&self.delegate_pk, &self.scope, self.expires_ms))?;
        root_vk.verify(&bytes, &sig)?;
        Ok(())
    }

    pub fn authorize_action(&self, kind: &GossipKind) -> Result<()> {
        match (&self.scope, kind) {
            (RoleScope::Overseer, _) => Ok(()),
            (RoleScope::Agent { agent_id, .. }, GossipKind::MailPublished(hdr)) => {
                if hdr.sender_agent == "OVERSEER" {
                    bail!("Agent cannot spoof OVERSEER messages");
                }
                if &hdr.sender_agent != agent_id {
                    bail!("Agent ID mismatch in MailPublished");
                }
                Ok(())
            }
            (
                RoleScope::Agent {
                    agent_id,
                    allowed_glob_prefix,
                },
                GossipKind::LeaseAnnounced(claim),
            ) => {
                if &claim.holder_agent != agent_id {
                    bail!("Agent cannot announce lease for another teammate");
                }
                if allowed_glob_prefix != "*" && !claim.glob_pattern.starts_with(allowed_glob_prefix) {
                    bail!("Lease pattern outside granted Meadowcap namespace prefix");
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// 배타적 파일 리스 클레임
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseClaim {
    pub glob_pattern: String,
    pub holder_agent: String,
    pub holder_pk: [u8; 32],
    pub acquired_hlc: HlcTimestamp,
    pub expires_wall_ms: u64,
    pub reason: String,
    pub overseer_override: bool,
}

/// 중앙 서버 없이 모든 피어가 동일한 잠금 테이블로 수렴하는 결정론적 파일 리스 중재기
#[derive(Default)]
pub struct DeterministicLeaseArbiter {
    active: BTreeMap<String, LeaseClaim>,
}

impl DeterministicLeaseArbiter {
    pub fn patterns_overlap(a: &str, b: &str) -> bool {
        if a == b || glob_match(a, b) || glob_match(b, a) {
            return true;
        }
        let pa = a.split('*').next().unwrap_or("").trim_end_matches('/');
        let pb = b.split('*').next().unwrap_or("").trim_end_matches('/');
        !pa.is_empty() && !pb.is_empty() && (a.starts_with(&format!("{pb}/")) || b.starts_with(&format!("{pa}/")))
    }

    pub fn purge_expired(&mut self, now_ms: u64) {
        self.active.retain(|_, c| c.expires_wall_ms > now_ms);
    }

    /// 오프라인/동시 선점 경합 시 결정론적 승자 판정:
    /// 1) Overseer 강제 오버라이드가 우선
    /// 2) 그렇지 않으면 더 앞선 `(acquired_hlc, holder_pk)` 클레임이 승리!
    pub fn apply_claim(&mut self, incoming: LeaseClaim, now_ms: u64) -> Result<(), LeaseClaim> {
        self.purge_expired(now_ms);

        let mut losers = Vec::new();
        for (pat, existing) in &self.active {
            if existing.holder_agent != incoming.holder_agent
                && Self::patterns_overlap(pat, &incoming.glob_pattern)
            {
                let incoming_wins = if incoming.overseer_override != existing.overseer_override {
                    incoming.overseer_override
                } else {
                    (incoming.acquired_hlc, incoming.holder_pk)
                        < (existing.acquired_hlc, existing.holder_pk)
                };

                if incoming_wins {
                    losers.push(pat.clone());
                } else {
                    return Err(existing.clone());
                }
            }
        }

        for k in losers {
            self.active.remove(&k);
        }
        self.active.insert(incoming.glob_pattern.clone(), incoming);
        Ok(())
    }

    pub fn release(&mut self, holder_agent: &str, glob_pattern: &str) -> bool {
        if let Some(c) = self.active.get(glob_pattern) {
            if c.holder_agent == holder_agent {
                self.active.remove(glob_pattern);
                return true;
            }
        }
        false
    }

    pub fn check_path(&mut self, agent_id: &str, path: &str, now_ms: u64) -> Option<LeaseClaim> {
        self.purge_expired(now_ms);
        let norm = path.trim_start_matches("./");
        for claim in self.active.values() {
            if claim.holder_agent != agent_id && Self::patterns_overlap(&claim.glob_pattern, norm) {
                return Some(claim.clone());
            }
        }
        None
    }

    pub fn snapshot(&mut self, now_ms: u64) -> Vec<LeaseClaim> {
        self.purge_expired(now_ms);
        self.active.values().cloned().collect()
    }
}

/// Loro CRDT 기반 무충돌 5인 작업 보드 & 결재 큐 문서
pub struct TeamCrdtDoc {
    pub doc: LoroDoc,
}

impl TeamCrdtDoc {
    pub fn new(peer_id_u64: u64) -> Self {
        let doc = LoroDoc::new();
        let _ = doc.set_peer_id(peer_id_u64);
        Self { doc }
    }

    pub fn upsert_agent_state(
        &self,
        agent_id: &str,
        name: &str,
        role: &str,
        engine: &str,
        task: &str,
        progress: i64,
        status: &str,
    ) -> Result<Vec<u8>> {
        let vv_before = self.doc.oplog_vv();
        let agents_map = self.doc.get_map("agents");
        let entry = serde_json::json!({
            "id": agent_id,
            "name": name,
            "role": role,
            "engine": engine,
            "task": task,
            "progress": progress,
            "status": status,
        });
        agents_map.insert(agent_id, entry.to_string())?;
        self.doc.commit();
        Ok(self.doc.export(ExportMode::updates(&vv_before))?)
    }

    pub fn import_delta(&self, bytes: &[u8]) -> Result<()> {
        self.doc.import(bytes)?;
        Ok(())
    }

    pub fn export_snapshot(&self) -> Result<Vec<u8>> {
        Ok(self.doc.export(ExportMode::Snapshot)?)
    }

    pub fn list_agents_json(&self) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        let val = self.doc.get_map("agents").get_deep_value();
        if let LoroValue::Map(m) = val {
            for (_k, v) in m.iter() {
                if let LoroValue::String(s) = v {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                        out.push(parsed);
                    }
                }
            }
        }
        out
    }
}
