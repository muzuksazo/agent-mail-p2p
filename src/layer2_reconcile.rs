//! Layer 2: Epidemic Gossip Wire Envelope & Willow-Inspired Range Set Reconciliation
//!
//! - 살아 있는 피어끼리는 `iroh-gossip`(HyParView + Plumtree)으로 밀리초 단위 전파합니다.
//! - `iroh-gossip`의 콘텐츠 해시 중복 제거에 걸리지 않도록 모든 봉투는 단조 증가 `(HlcTimestamp, nonce)`와
//!   Ed25519 서명을 포함합니다.
//! - 오프라인이었다가 뒤늦게 합류한 피어는 전체 로그를 다 받지 않고,
//!   시간 구간 `[start_ms, end_ms)`의 256비트 XOR BLAKE3 지문(`RangeFingerprint`)을 비교해
//!   지문이 다른 구간의 누락 엔트리만 O(diff)로 정밀 회수합니다.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::layer3_state_auth::{CapabilityCert, HlcTimestamp, LeaseClaim};

/// 4KB 이하 본문은 가십 봉투에 인라인 탑재, 초과분은 BLAKE3 해시 티켓만 전송 후 QUIC Pull
pub const INLINE_BODY_MAX_BYTES: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailHeader {
    pub id: String,
    pub sender_agent: String,
    pub recipient: String, // "ALL", "OVERSEER", 또는 에이전트 ID ("pm","fe","be","db","qa")
    pub priority: MailPriority,
    pub hint: String,
    pub body_blake3: [u8; 32],
    pub body_len: u32,
    /// 소형 메시지는 즉시 원자적 가시성을 얻도록 인라인 포함, 대형 첨부는 None -> QUIC Blob Fetch
    pub inline_body: Option<String>,
    pub hlc: HlcTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailPriority {
    Normal,
    Urgent,
}

impl MailPriority {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Urgent => "urgent",
        }
    }
}

/// 가십 망을 타고 전파되는 상위 페이로드 종류
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipKind {
    /// 새 메일 발행 (팀장 긴급 지시 포함)
    MailPublished(MailHeader),
    /// 수신 상태 전이 ('picked' 또는 'ack' + 진행률)
    ReceiptUpdate {
        msg_id: String,
        agent_id: String,
        new_status: String,
        outcome: String,
        progress: Option<u8>,
        hlc: HlcTimestamp,
    },
    /// 아직 'sent' 상태인 메시지의 발신자 회수(Withdraw)
    MailWithdrawn {
        msg_id: String,
        hlc: HlcTimestamp,
    },
    /// HLC 결정론적 배타 파일 리스 선언 또는 갱신(하트비트)
    LeaseAnnounced(LeaseClaim),
    /// 파일 리스 명시적 반납
    LeaseReleased {
        glob_pattern: String,
        holder_agent: String,
        hlc: HlcTimestamp,
    },
    /// Loro CRDT 증분 바이트 델타 (에이전트 로스터, 작업 분담 보드, NeedsYou 결재 카드 동기화)
    LoroDelta {
        delta_bytes: Vec<u8>,
        hlc: HlcTimestamp,
    },
    /// 주기적 안티엔트로피 범위 지문 비콘 (네트워크 파티션 치유 및 누락 감지 트리거)
    RangeBeacon {
        total_range: TimeRange,
        fingerprint: [u8; 32],
    },
}

/// Ed25519 서명 및 Meadowcap 스타일 권한 증명서가 결합된 가십 봉투
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedGossipEnvelope {
    pub author_pk: [u8; 32],
    pub nonce: u64,
    pub cert: CapabilityCert,
    pub payload_bytes: Vec<u8>, // 직렬화된 GossipKind
    pub signature: Vec<u8>,     // 64바이트 Ed25519 서명
}

impl SignedGossipEnvelope {
    pub fn sign_new(
        signing_key: &SigningKey,
        nonce: u64,
        cert: CapabilityCert,
        kind: &GossipKind,
    ) -> anyhow::Result<Self> {
        let payload_bytes = postcard::to_stdvec(kind)?;
        let mut sig_input = Vec::with_capacity(8 + payload_bytes.len());
        sig_input.extend_from_slice(&nonce.to_le_bytes());
        sig_input.extend_from_slice(&payload_bytes);
        let sig: Signature = signing_key.sign(&sig_input);

        Ok(Self {
            author_pk: signing_key.verifying_key().to_bytes(),
            nonce,
            cert,
            payload_bytes,
            signature: sig.to_bytes().to_vec(),
        })
    }

    /// 봉투 서명 및 루트 Overseer가 위임한 Meadowcap 권한 범위까지 한 번에 검증
    pub fn verify_and_decode(&self, overseer_root_pk: &[u8; 32], now_ms: u64) -> anyhow::Result<GossipKind> {
        let vk = VerifyingKey::from_bytes(&self.author_pk)?;
        let sig_arr: [u8; 64] = self
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid signature length"))?;
        let sig = Signature::from_bytes(&sig_arr);

        let mut sig_input = Vec::with_capacity(8 + self.payload_bytes.len());
        sig_input.extend_from_slice(&self.nonce.to_le_bytes());
        sig_input.extend_from_slice(&self.payload_bytes);
        vk.verify(&sig_input, &sig)?;

        // 권한 인증서가 실제 이 작성자 공개키(`author_pk`)에 발급되었는지 검증
        self.cert.verify(overseer_root_pk, &self.author_pk, now_ms)?;

        let kind: GossipKind = postcard::from_bytes(&self.payload_bytes)?;
        // 권한 범위 검사 (예: OVERSEER 발신이나 결재 승인은 Overseer 역할 인증서 필수!)
        self.cert.authorize_action(&kind)?;
        Ok(kind)
    }
}

/// Willow 스타일 반열린 시간 구간 `[start_ms, end_ms)`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ms: u64,
    pub end_ms: u64,
}

/// 순서 무관(Commutative) 대수적 지문: 엔트리 ID와 본문 해시의 BLAKE3 다이제스트를 XOR 누적
pub fn xor_fingerprint<'a>(items: impl Iterator<Item = (&'a str, &'a [u8; 32])>) -> [u8; 32] {
    let mut acc = [0u8; 32];
    for (id, body_hash) in items {
        let mut hasher = blake3::Hasher::new();
        hasher.update(id.as_bytes());
        hasher.update(body_hash);
        let digest = hasher.finalize();
        for (a, b) in acc.iter_mut().zip(digest.as_bytes().iter()) {
            *a ^= *b;
        }
    }
    acc
}

/// QUIC `SYNC_BLOB_ALPN` 요청 프로토콜
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReconcileRequest {
    /// 특정 시간 구간의 지문 및 엔트리 수 조회 (불일치 시 이진 분할 또는 엔트리 요청)
    CompareRange {
        range: TimeRange,
        caller_fingerprint: [u8; 32],
    },
    /// 특정 구간 내 내가 갖지 못한 메시지 헤더+본문 일괄 요청
    FetchRangeEntries {
        range: TimeRange,
        known_ids: Vec<String>,
    },
    /// 대용량 본문(BLAKE3 해시 지정) 단건 스트리밍 조회
    FetchBlob {
        blake3_hash: [u8; 32],
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReconcileResponse {
    RangeMatched,
    RangeMismatch {
        left_half: (TimeRange, [u8; 32], u32),
        right_half: (TimeRange, [u8; 32], u32),
    },
    MissingEntries {
        entries: Vec<(MailHeader, String)>,
        loro_snapshot: Vec<u8>,
    },
    BlobPayload {
        found: bool,
        content: Option<String>,
    },
}
