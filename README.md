# `agent-mail-p2p` (Rust 5계층 무서버 P2P Agent Mail & MCP 버스)

클라우드 서버나 외부 DB 가입 없이 5대의 팀원 PC가 **Iroh QUIC 홀펀칭 + Plumtree 가십 + 범위 지문 부분 동기화 + Loro CRDT + HLC 결정론적 파일 리스 + 로컬 SQLite WAL/FTS5**로 직결되는 단일 바이너리 Rust 프로젝트입니다.


## 맥 앱으로 쓰기 (가장 쉬움)

1. [Releases](https://github.com/muzuksazo/agent-mail-p2p/releases)에서 `AgentMail-x.y.z.dmg`를 받아 열고, **Agent Mail**을 **응용 프로그램** 폴더로 끌어다 놓아요.
2. **Agent Mail**을 더블클릭하면 브라우저에 화면이 열려요.
3. 이름을 적고 **방 만들기** → 🎟️ **친구 초대**에서 **초대 코드 복사** → 친구에게 보내요.
   친구는 앱을 열고 **초대받은 방 들어가기**에 붙여 넣으면 끝이에요.

다음부터는 앱을 열면 같은 방으로 바로 들어가요. Claude Code 연결은 화면의 🤖 **Claude 연결** 버튼 하나로 돼요.
끄기·다른 방으로 옮기기는 화면 오른쪽 위 `⋯`에 있어요.

아래는 터미널에서 직접 쓰는 방법(개발자용)이에요.

## 빌드 방법 (Windows / Linux / WSL 공통)
`rusqlite`가 `bundled` 모드로 포함되어 있어 시스템에 SQLite가 없어도 한 줄로 단일 실행 파일이 생성됩니다.
```bash
cargo build --release
```

## 5인 팀플 실행 시나리오

### 1. 팀장 PC: 스웜 개설 및 입장 티켓 발급
```bash
./target/release/agent-mail-p2p open --agent pm --name 민준 --port 18765
```
터미널에 출력되는 `amp2p_...` 티켓 문자열만 카톡이나 디코로 팀원 4명에게 전달하면 끝입니다.

### 2. 팀원 4명 PC: 공유기(NAT) 뒤에서 티켓 하나로 직결 합류
```bash
./target/release/agent-mail-p2p join amp2p_... --agent be --name 도윤 --allowed-glob "src/api/" --port 18766
```
오프라인이었다가 나중에 켜도 `RangeBeacon`과 `SYNC_BLOB_ALPN` 스트림을 통해 못 받은 대화와 Loro 작업 보드가 자동으로 복구됩니다.

### 3. 로컬 Claude Code / Codex / Cursor 연결 (투트랙 하이브리드)
- **Claude Code 실시간 Channel Push + MCP 툴 연결**:
  `.mcp.json`에 `agent-mail-p2p mcp --agent be --daemon-url http://127.0.0.1:18766` 등록 후 실행:
  ```bash
  claude --dangerously-load-development-channels server:mail-be
  ```
- **Codex / Cursor / Gemini CLI 훅 연결**:
  `PostToolUse` 훅에 `agent-mail-p2p hook post-tool --agent fe --daemon-url http://127.0.0.1:18765` 등록
