//! Layer 1: Discovery & QUIC Transport (`iroh` 1.2 + QNT Hole-Punching + Swarm Ticket)
//!
//! - IP 주소 대신 Ed25519 공개키(`EndpointId`)로 피어를 호출합니다.
//! - `presets::N0` 설정을 통해 UDP 홀펀칭(QUIC-NAT-Traversal), Pkarr DNS 조회,
//!   대칭형 NAT 환경에서의 E2EE 무상태 릴레이 폴백을 자동으로 수행합니다.
//! - 하나의 QUIC `Endpoint` 위에 실시간 가십(`iroh_gossip::ALPN`)과
//!   오프라인 복구용 범위 동기화·블롭 전송(`SYNC_BLOB_ALPN`) 두 프로토콜을 멀티플렉싱합니다.

use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use data_encoding::BASE32_NOPAD;
use iroh::endpoint::{presets, Connection};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};

use crate::layer2_reconcile::{ReconcileRequest, ReconcileResponse};
use crate::layer4_sqlite_view::SqliteProjection;

/// 오프라인 복구(Range Set Reconciliation) 및 대용량 본문(BLAKE3 Blob) 스트리밍용 ALPN
pub const SYNC_BLOB_ALPN: &[u8] = b"agent-mail-p2p/sync-blob/1";

/// 팀원 5명이 공유하는 단일 입장 티켓 (Base32 직렬화)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmTicket {
    /// 32바이트 iroh-gossip 토픽 식별자
    pub topic: TopicId,
    /// 팀장(Overseer) 루트 Ed25519 공개키 (Meadowcap 권한 검증 루트)
    pub overseer_root_pk: [u8; 32],
    /// 초기 부트스트랩 피어 주소 목록 (EndpointId + RelayUrl + Direct UDP 소켓)
    pub bootstrap: Vec<EndpointAddr>,
}

impl SwarmTicket {
    pub fn encode(&self) -> Result<String> {
        let raw = postcard::to_stdvec(self).context("Failed to serialize SwarmTicket")?;
        Ok(format!("amp2p_{}", BASE32_NOPAD.encode(&raw).to_lowercase()))
    }
}

impl FromStr for SwarmTicket {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let body = s.trim().strip_prefix("amp2p_").unwrap_or(s.trim());
        let bytes = BASE32_NOPAD
            .decode(body.to_uppercase().as_bytes())
            .context("Invalid Base32 SwarmTicket")?;
        postcard::from_bytes(&bytes).context("Failed to decode SwarmTicket payload")
    }
}

impl fmt::Display for SwarmTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.encode() {
            Ok(s) => write!(f, "{s}"),
            Err(_) => write!(f, "<invalid-ticket>"),
        }
    }
}

/// 영속적인 노드 Ed25519 비밀키 로드 또는 최초 생성
pub fn load_or_create_secret_key(key_file: &Path) -> Result<SecretKey> {
    if key_file.exists() {
        let hex_or_bytes = std::fs::read(key_file)?;
        if hex_or_bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&hex_or_bytes);
            return Ok(SecretKey::from_bytes(&arr));
        }
    }
    if let Some(parent) = key_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // iroh 1.x: generate()는 내부 OS 난수원을 쓰며 인자를 받지 않는다.
    let key = SecretKey::generate();
    std::fs::write(key_file, key.to_bytes())?;
    Ok(key)
}

/// QUIC 직결 스트림을 통해 범위 지문 비교(Range Set Reconciliation)와 BLAKE3 본문 청크를 응답하는 핸들러
#[derive(Clone)]
pub struct SyncBlobProtocol {
    pub store: Arc<SqliteProjection>,
}

// iroh 1.x의 ProtocolHandler는 Debug를 요구한다. SQLite 연결 내부는 출력하지 않는다.
impl fmt::Debug for SyncBlobProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncBlobProtocol").finish_non_exhaustive()
    }
}

impl ProtocolHandler for SyncBlobProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        let store = self.store.clone();
        tokio::spawn(async move {
            if let Ok((mut send, mut recv)) = connection.accept_bi().await {
                if let Ok(req_bytes) = recv.read_to_end(4 * 1024 * 1024).await {
                    if let Ok(req) = postcard::from_bytes::<ReconcileRequest>(&req_bytes) {
                        let resp: ReconcileResponse = store.handle_reconcile_request(req);
                        if let Ok(out) = postcard::to_stdvec(&resp) {
                            let _ = send.write_all(&out).await;
                            let _ = send.finish();
                        }
                    }
                }
            }
            // 연결을 여기서 바로 버리면 QUIC이 즉시 닫혀 아직 전달되지 않은 응답이 사라진다.
            // 요청한 쪽이 응답을 다 읽고 연결을 닫을 때까지 기다린다(상대가 사라진 경우 대비 10초 제한).
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), connection.closed()).await;
        });
        Ok(())
    }
}

/// 실행 중인 Layer 1 P2P 네트워크 노드 핸들
pub struct P2pTransportNode {
    pub endpoint: Endpoint,
    pub gossip: Gossip,
    pub router: Router,
}

impl P2pTransportNode {
    pub async fn bind(secret_key: SecretKey, store: Arc<SqliteProjection>) -> Result<Self> {
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .bind()
            .await
            .context("Failed to bind Iroh QUIC endpoint")?;

        let gossip = Gossip::builder().spawn(endpoint.clone());
        let sync_proto = SyncBlobProtocol { store };

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(SYNC_BLOB_ALPN, sync_proto)
            .spawn();

        Ok(Self {
            endpoint,
            gossip,
            router,
        })
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// 원격 피어에게 직접 QUIC 양방향 스트림을 열어 누락된 메시지 범위나 대용량 BLAKE3 본문을 복구
    pub async fn request_reconcile(
        &self,
        peer: EndpointAddr,
        req: &ReconcileRequest,
    ) -> Result<ReconcileResponse> {
        let conn = self
            .endpoint
            .connect(peer, SYNC_BLOB_ALPN)
            .await
            .context("QUIC connect for SYNC_BLOB_ALPN failed")?;
        let (mut send, mut recv) = conn.open_bi().await?;
        let raw = postcard::to_stdvec(req)?;
        send.write_all(&raw).await?;
        send.finish()?;
        let resp_bytes = recv.read_to_end(16 * 1024 * 1024).await?;
        Ok(postcard::from_bytes(&resp_bytes)?)
    }
}
