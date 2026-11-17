//! `agent-mail-p2p` 단일 실행 바이너리 엔트리포인트
//!
//! 사용 예시:
//!   1) 팀장(방 개설 + Overseer 루트 키 생성):
//!      `agent-mail-p2p open --agent pm --name 민준 --port 18765`
//!      -> 콘솔에 `amp2p_...` 스웜 티켓 출력!
//!   2) 팀원 4명(각자 집/학교 공유기 뒤에서 티켓 하나로 P2P 직결 합류):
//!      `agent-mail-p2p join <TICKET> --agent be --name 도윤 --port 18766`
//!   3) Claude Code / Codex / Cursor 로컬 MCP 브리지 연결:
//!      `agent-mail-p2p mcp --agent be --daemon-url http://127.0.0.1:18766`

mod layer1_transport;
mod layer2_reconcile;
mod layer3_state_auth;
mod layer4_sqlite_view;
mod layer5_mcp_bridge;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use ed25519_dalek::SigningKey;
use futures_lite::StreamExt;
use iroh_gossip::api::Event as GossipEvent;
use iroh_gossip::proto::TopicId;
use tokio::sync::mpsc;

use crate::layer1_transport::{load_or_create_secret_key, P2pTransportNode, SwarmTicket};
use crate::layer2_reconcile::{GossipKind, ReconcileRequest, ReconcileResponse, SignedGossipEnvelope, TimeRange};
use crate::layer3_state_auth::{now_unix_ms, CapabilityCert, HybridClock, RoleScope};
use crate::layer4_sqlite_view::SqliteProjection;
use crate::layer5_mcp_bridge::{
    build_local_http_router, reqwest_free_http_get, run_stdio_mcp_bridge, LocalBridgeState,
};

#[derive(Parser)]
#[command(name = "agent-mail-p2p", version, about = "5-Layer Serverless P2P Agent Mail & MCP Bus")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 새 5인 팀플 P2P 스웜 개설 (Overseer 루트 키 생성 및 입장 티켓 발급)
    Open {
        #[arg(long, default_value = "pm")]
        agent: String,
        #[arg(long, default_value = "민준")]
        name: String,
        #[arg(long, default_value_t = 18765)]
        port: u16,
        #[arg(long, default_value = ".agent_mail_p2p")]
        data_dir: PathBuf,
    },
    /// 기존 P2P 스웜에 티켓 하나로 홀펀칭 합류 + 오프라인 누락 메시지 자동 복구
    Join {
        ticket: String,
        #[arg(long)]
        agent: String,
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "*")]
        allowed_glob: String,
        #[arg(long, default_value_t = 18765)]
        port: u16,
        #[arg(long, default_value = ".agent_mail_p2p")]
        data_dir: PathBuf,
    },
    /// 로컬 에이전트용 stdio MCP 서버 + notifications/claude/channel 푸시 브리지
    Mcp {
        #[arg(long)]
        agent: String,
        #[arg(long, default_value = "http://127.0.0.1:18765")]
        daemon_url: String,
        #[arg(long)]
        no_channel_push: bool,
    },
    /// PostToolUse / PreToolUse 라이프사이클 훅 실행
    Hook {
        mode: String,
        #[arg(long)]
        agent: String,
        #[arg(long, default_value = "http://127.0.0.1:18765")]
        daemon_url: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.cmd {
        Command::Mcp {
            agent,
            daemon_url,
            no_channel_push,
        } => run_stdio_mcp_bridge(&daemon_url, &agent, !no_channel_push),

        Command::Hook {
            mode,
            agent,
            daemon_url,
        } => {
            if mode == "post-tool" {
                let res = reqwest_free_http_get(&format!(
                    "{daemon_url}/api/wait?agent={agent}&timeout=0"
                ))?;
                if let Some(hints) = res.get("hints").and_then(|v| v.as_array()) {
                    if !hints.is_empty() {
                        let summary: Vec<String> = hints
                            .iter()
                            .map(|h| {
                                format!(
                                    "@{}: {}",
                                    h.get("sender").and_then(|v| v.as_str()).unwrap_or("peer"),
                                    h.get("hint").and_then(|v| v.as_str()).unwrap_or("")
                                )
                            })
                            .collect();
                        let out = serde_json::json!({
                            "hookSpecificOutput": {
                                "hookEventName": "PostToolUse",
                                "additionalContext": format!("[P2P 단톡 새 메일] {} -> fetch_inbox로 확인하세요.", summary.join(" | "))
                            }
                        });
                        println!("{out}");
                    }
                }
            }
            Ok(())
        }

        Command::Open {
            agent,
            name,
            port,
            data_dir,
        } => {
            let iroh_sk = load_or_create_secret_key(&data_dir.join(format!("{agent}.key")))?;
            let signing_key = SigningKey::from_bytes(&iroh_sk.to_bytes());
            let root_pk = signing_key.verifying_key().to_bytes();
            let cert = CapabilityCert::issue(
                &signing_key,
                root_pk,
                RoleScope::Overseer,
                30 * 24 * 3600 * 1000,
            )?;

            let topic_bytes: [u8; 32] = rand::random();
            let topic = TopicId::from_bytes(topic_bytes);

            run_p2p_daemon(
                agent,
                name,
                true,
                iroh_sk,
                signing_key,
                root_pk,
                cert,
                topic,
                Vec::new(),
                port,
                data_dir,
            )
            .await
        }

        Command::Join {
            ticket,
            agent,
            name,
            allowed_glob,
            port,
            data_dir,
        } => {
            let parsed = SwarmTicket::from_str(&ticket)?;
            let iroh_sk = load_or_create_secret_key(&data_dir.join(format!("{agent}.key")))?;
            let signing_key = SigningKey::from_bytes(&iroh_sk.to_bytes());
            let my_pk = signing_key.verifying_key().to_bytes();

            // 데모 편의상 자체 서명 에이전트 인증서 발급 (프로덕션에서는 팀장이 서명한 위임 토큰 문자열을 전달)
            let cert = CapabilityCert::issue(
                &signing_key,
                my_pk,
                RoleScope::Agent {
                    agent_id: agent.clone(),
                    allowed_glob_prefix: allowed_glob,
                },
                7 * 24 * 3600 * 1000,
            )?;

            run_p2p_daemon(
                agent,
                name,
                false,
                iroh_sk,
                signing_key,
                parsed.overseer_root_pk,
                cert,
                parsed.topic,
                parsed.bootstrap,
                port,
                data_dir,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_p2p_daemon(
    agent_id: String,
    agent_name: String,
    is_overseer: bool,
    iroh_sk: iroh::SecretKey,
    signing_key: SigningKey,
    overseer_root_pk: [u8; 32],
    cert: CapabilityCert,
    topic: TopicId,
    bootstrap_addrs: Vec<iroh::EndpointAddr>,
    port: u16,
    data_dir: PathBuf,
) -> Result<()> {
    let my_pk = signing_key.verifying_key().to_bytes();
    let peer_u64 = u64::from_le_bytes(my_pk[..8].try_into().unwrap());
    let store = Arc::new(SqliteProjection::open(
        &data_dir.join(format!("{agent_id}_mail.sqlite3")),
        peer_u64,
    )?);

    let initial_delta = store.upsert_local_agent(
        &agent_id,
        &agent_name,
        if is_overseer { "팀장 · 아키텍트" } else { "팀원 에이전트" },
        "claude-code",
        "P2P 스웜 협업 중",
        10,
        "working",
    )?;

    let node = Arc::new(P2pTransportNode::bind(iroh_sk, store.clone()).await?);

    // 다른 팀원들이 복사해 합류할 수 있는 P2P 입장 티켓 출력
    let my_addr = node.endpoint.addr();
    let ticket = SwarmTicket {
        topic,
        overseer_root_pk,
        bootstrap: vec![my_addr],
    };
    eprintln!("===============================================================");
    eprintln!("[P2P Node Active] EndpointId : {}", node.endpoint_id());
    eprintln!("[Swarm Join Ticket]          : {}", ticket.encode()?);
    eprintln!("[Local Group-Chat & MCP API] : http://127.0.0.1:{port}");
    eprintln!("===============================================================");

    let mut bootstrap_ids = Vec::new();
    for addr in &bootstrap_addrs {
        bootstrap_ids.push(addr.id);
    }

    let gossip_topic = if bootstrap_ids.is_empty() {
        node.gossip.subscribe(topic, vec![]).await?
    } else {
        node.gossip.subscribe_and_join(topic, bootstrap_ids).await?
    };
    let (gossip_sender, mut gossip_receiver) = gossip_topic.split();

    let (gossip_out_tx, mut gossip_out_rx) = mpsc::channel::<GossipKind>(256);
    let _ = gossip_out_tx
        .send(GossipKind::LoroDelta {
            delta_bytes: initial_delta,
            hlc: HybridClock::new(&my_pk).tick(),
        })
        .await;

    // 1) 송신 루프: 로컬 액션 -> 서명된 가십 봉투(SignedGossipEnvelope) -> Plumtree 브로드캐스트
    let nonce_counter = Arc::new(AtomicU64::new(1));
    let sk_clone = signing_key.clone();
    let cert_clone = cert.clone();
    let nonce_c = nonce_counter.clone();
    let sender_clone = gossip_sender.clone();
    tokio::spawn(async move {
        while let Some(kind) = gossip_out_rx.recv().await {
            let n = nonce_c.fetch_add(1, Ordering::Relaxed);
            if let Ok(env) = SignedGossipEnvelope::sign_new(&sk_clone, n, cert_clone.clone(), &kind)
            {
                if let Ok(wire) = postcard::to_stdvec(&env) {
                    let _ = sender_clone.broadcast(wire.into()).await;
                }
            }
        }
    });

    // 2) 주기적 안티엔트로피 비콘: 15초마다 전체 시간 범위의 XOR 지문을 브로드캐스트해 누락을 즉시 감지
    let store_beacon = store.clone();
    let beacon_tx = gossip_out_tx.clone();
    let beacon_agent = agent_id.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            // 리스는 범위 동기화 대상이 아니므로, 쥐고 있는 쪽이 주기적으로 다시 알린다.
            // 늦게 합류했거나 재시작한 팀원도 다음 주기 안에 같은 리스 테이블로 수렴한다(같은 클레임은 멱등).
            for claim in store_beacon.leases_held_by(&beacon_agent) {
                let _ = beacon_tx.send(GossipKind::LeaseAnnounced(claim)).await;
            }
            let range = TimeRange {
                start_ms: 0,
                end_ms: now_unix_ms() + 60_000,
            };
            let (fp, _cnt) = store_beacon.range_fingerprint(range);
            let _ = beacon_tx
                .send(GossipKind::RangeBeacon {
                    total_range: range,
                    fingerprint: fp,
                })
                .await;
        }
    });

    // 3) 수신 루프: Plumtree 가십 수신 -> 서명 검증 -> Consistent Visibility 커밋 또는 QUIC 본문/범위 복구!
    let store_recv = store.clone();
    let node_recv = node.clone();
    let boot_peers = bootstrap_addrs.clone();
    tokio::spawn(async move {
        while let Some(Ok(ev)) = gossip_receiver.next().await {
            if let GossipEvent::Received(msg) = ev {
                let Ok(env) = postcard::from_bytes::<SignedGossipEnvelope>(&msg.content) else {
                    continue;
                };
                // 데모 join 모드에서는 자체 서명도 허용할 수 있도록 작성자 키 또는 루트 키 검증
                let effective_root = if env.author_pk == overseer_root_pk {
                    overseer_root_pk
                } else {
                    env.author_pk
                };
                let Ok(kind) = env.verify_and_decode(&effective_root, now_unix_ms()) else {
                    continue;
                };

                match kind {
                    GossipKind::MailPublished(hdr) => {
                        let body_hash = hdr.body_blake3;
                        if let Ok(false) = store_recv.ingest_mail_header(hdr) {
                            // 인라인 본문이 없는 대용량 메시지 -> QUIC 스트림으로 본문을 당겨 온 뒤 원자적 커밋!
                            if let Some(peer) = boot_peers.first().cloned() {
                                let st = store_recv.clone();
                                let nd = node_recv.clone();
                                tokio::spawn(async move {
                                    if let Ok(ReconcileResponse::BlobPayload {
                                        found: true,
                                        content: Some(body),
                                    }) = nd
                                        .request_reconcile(
                                            peer,
                                            &ReconcileRequest::FetchBlob {
                                                blake3_hash: body_hash,
                                            },
                                        )
                                        .await
                                    {
                                        let _ = st.fulfill_staged_blob(body_hash, &body);
                                    }
                                });
                            }
                        }
                    }
                    GossipKind::ReceiptUpdate {
                        msg_id,
                        new_status,
                        outcome,
                        ..
                    } => {
                        let _ = store_recv.apply_receipt_update(&msg_id, &new_status, &outcome);
                    }
                    GossipKind::MailWithdrawn { msg_id, .. } => {
                        let _ = store_recv.apply_withdraw(&msg_id);
                    }
                    GossipKind::LeaseAnnounced(claim) => {
                        let _ = store_recv.apply_lease_claim(claim);
                    }
                    GossipKind::LeaseReleased {
                        glob_pattern,
                        holder_agent,
                        ..
                    } => {
                        store_recv.release_lease(&holder_agent, &glob_pattern);
                    }
                    GossipKind::LoroDelta { delta_bytes, .. } => {
                        let _ = store_recv.merge_loro_delta(&delta_bytes);
                    }
                    GossipKind::RangeBeacon {
                        total_range,
                        fingerprint,
                    } => {
                        let (local_fp, _) = store_recv.range_fingerprint(total_range);
                        if local_fp != fingerprint {
                            if let Some(peer) = boot_peers.first().cloned() {
                                let st = store_recv.clone();
                                let nd = node_recv.clone();
                                tokio::spawn(async move {
                                    let r = nd
                                        .request_reconcile(
                                            peer,
                                            &ReconcileRequest::FetchRangeEntries {
                                                range: total_range,
                                                known_ids: Vec::new(),
                                            },
                                        )
                                        .await;
                                    if let Err(e) = &r {
                                        tracing::warn!("range reconcile with bootstrap peer failed: {e:#}");
                                    }
                                    if let Ok(ReconcileResponse::MissingEntries {
                                        entries,
                                        loro_snapshot,
                                    }) = r
                                    {
                                        let _ = st.merge_loro_delta(&loro_snapshot);
                                        for (hdr, body) in entries {
                                            let _ = st.commit_verified_mail(&hdr, &body);
                                        }
                                    }
                                });
                            }
                        }
                    }
                }
            }
        }
    });

    // 4) 로컬 HTTP/SSE 브리지 서버 가동 (127.0.0.1:{port})
    let bridge_state = Arc::new(LocalBridgeState {
        agent_id,
        node_pk: my_pk,
        is_overseer,
        store,
        clock: std::sync::Mutex::new(HybridClock::new(&my_pk)),
        gossip_out_tx,
    });

    let app = build_local_http_router(bridge_state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    axum::serve(listener, app).await?;
    node.router.shutdown().await?;
    Ok(())
}
