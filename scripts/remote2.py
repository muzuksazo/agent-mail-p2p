#!/usr/bin/env python3
"""이 컴퓨터(pm)와 SSH 원격 컴퓨터(be) 두 대로 하는 전체 시험.

사용법: python3 scripts/remote2.py <ssh-host> <원격 실행 파일 경로> [로컬 실행 파일]
- 로컬: 임시 폴더 + 18775 포트에 pm 노드를 띄운다.
- 원격: ~/amp2p-test/run 폴더 + 18776 포트에 be 노드를 띄운다(nohup).
- 끝나면 양쪽에서 자기가 띄운 프로세스만 끈다.
"""
from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

HOST = sys.argv[1]
RBIN = sys.argv[2]
LBIN = sys.argv[3] if len(sys.argv) > 3 else str(Path(__file__).resolve().parent.parent / "target/release/agent-mail-p2p")
PM_PORT, BE_PORT = 18775, 18776
RDIR = "~/amp2p-test/run"
results: list[tuple[str, bool, str]] = []


def ssh(cmd: str, timeout: int = 60, input_text: str | None = None) -> str:
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", HOST, cmd], capture_output=True, text=True, timeout=timeout, input=input_text)
    return r.stdout


def local_state() -> dict:
    return json.load(urllib.request.urlopen(f"http://127.0.0.1:{PM_PORT}/api/state", timeout=3))


def remote_state() -> dict:
    out = ssh(f"curl -s --max-time 3 http://127.0.0.1:{BE_PORT}/api/state", 20)
    return json.loads(out) if out.strip() else {"agents": [], "messages": [], "leases": []}


def local_post(path: str, body: dict) -> dict:
    req = urllib.request.Request(f"http://127.0.0.1:{PM_PORT}{path}", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=5))


def remote_post(path: str, body: dict) -> dict:
    out = ssh(f"curl -s --max-time 5 -X POST http://127.0.0.1:{BE_PORT}{path} -H 'Content-Type: application/json' -d {shlex.quote(json.dumps(body))}", 20)
    return json.loads(out or "{}")


def until(pred, timeout: float) -> tuple[bool, float]:
    start = time.time()
    while time.time() - start < timeout:
        try:
            if pred():
                return True, time.time() - start
        except Exception:
            pass
        time.sleep(1)
    return False, time.time() - start


def record(name: str, ok: bool, note: str = "") -> None:
    results.append((name, ok, note))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {note}", flush=True)


def has_msg(st: dict, text: str) -> bool:
    return any(m.get("body_md") == text for m in st.get("messages", []))


def start_remote(ticket: str) -> None:
    # SSH 세션에서 완전히 떼어 내야(입력·출력 모두 닫기) ssh가 바로 돌아온다.
    ssh(f"mkdir -p {RDIR} && cd {RDIR} && (setsid nohup {RBIN} join {ticket} --agent be --name 원격BE --port {BE_PORT} "
        f"--data-dir {RDIR}/data < /dev/null > {RDIR}/be.log 2>&1 & echo $! > {RDIR}/be.pid)", 20)


def stop_remote() -> None:
    ssh(f"test -f {RDIR}/be.pid && kill $(cat {RDIR}/be.pid) 2>/dev/null; rm -f {RDIR}/be.pid; true")


def remote_mcp(requests: list[dict], listen_secs: int) -> list[dict]:
    """원격에서 MCP stdio를 띄워 요청을 보내고 listen_secs 동안 나온 모든 줄을 돌려준다."""
    payload = "".join(json.dumps(r) + "\n" for r in requests)
    cmd = f"(cat; sleep {listen_secs}) | timeout {listen_secs + 5} {RBIN} mcp --agent be --daemon-url http://127.0.0.1:{BE_PORT}"
    out = ssh(cmd, listen_secs + 30, input_text=payload)
    lines = []
    for l in out.splitlines():
        try:
            lines.append(json.loads(l))
        except Exception:
            pass
    return lines


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="amp2p-remote-"))
    log = open(tmp / "pm.log", "w")
    pm = subprocess.Popen([LBIN, "open", "--agent", "pm", "--name", "로컬PM", "--port", str(PM_PORT), "--data-dir", str(tmp / "pm")],
                          stdout=subprocess.DEVNULL, stderr=log)
    ssh(f"rm -rf {RDIR}; mkdir -p {RDIR}")
    try:
        until(lambda: local_state() is not None, 20)
        ticket = ""
        for _ in range(50):
            m = re.search(r"amp2p_[a-z0-9]+", (tmp / "pm.log").read_text())
            if m:
                ticket = m.group(0)
                break
            time.sleep(0.2)

        # 1) 합류 전 기록
        local_post("/api/send", {"recipient": "ALL", "body_md": "합류 전 메시지", "priority": "normal"})
        local_post("/api/leases/reserve", {"agent_id": "pm", "glob_pattern": "docs/*"})
        start_remote(ticket)
        ok, _ = until(lambda: remote_state() is not None and ssh(f"curl -s -o /dev/null -w %{{http_code}} http://127.0.0.1:{BE_PORT}/") == "200", 30)
        record("원격 노드 기동", ok)

        ok, t = until(lambda: any(a["id"] == "pm" for a in remote_state()["agents"]), 45)
        record("원격이 로컬 방장을 봄", ok, f"{t:.0f}초")
        ok, t = until(lambda: any(a["id"] == "be" for a in local_state()["agents"]), 45)
        record("로컬이 원격 팀원을 봄", ok, f"{t:.0f}초")
        ok, t = until(lambda: has_msg(remote_state(), "합류 전 메시지"), 45)
        record("합류 전 메시지 원격 복구", ok, f"{t:.0f}초")
        ok, t = until(lambda: any(l["glob_pattern"] == "docs/*" for l in remote_state()["leases"]), 45)
        record("합류 전 리스 원격 복구", ok, f"{t:.0f}초")

        # 2) 양방향 메시지
        local_post("/api/send", {"recipient": "be", "body_md": "로컬→원격", "priority": "normal"})
        ok, t = until(lambda: has_msg(remote_state(), "로컬→원격"), 20)
        record("로컬→원격 메시지", ok, f"{t:.1f}초")
        remote_post("/api/send", {"recipient": "pm", "body_md": "원격→로컬", "priority": "normal"})
        ok, t = until(lambda: has_msg(local_state(), "원격→로컬"), 20)
        record("원격→로컬 메시지", ok, f"{t:.1f}초")

        # 3) 리스 충돌(원격이 로컬 점유 파일을 고치려 할 때)
        chk = json.loads(ssh(f"curl -s 'http://127.0.0.1:{BE_PORT}/api/leases/check?agent=be&path=docs/a.md'") or "{}")
        record("원격 편집이 로컬 리스에 막힘", chk.get("allowed") is False, json.dumps(chk, ensure_ascii=False)[:60])
        remote_post("/api/leases/reserve", {"agent_id": "be", "glob_pattern": "src/web/*"})
        ok, t = until(lambda: local_post("/api/leases/check", {}) if False else
                      json.load(urllib.request.urlopen(f"http://127.0.0.1:{PM_PORT}/api/leases/check?agent=pm&path=src/web/x.ts")).get("allowed") is False, 30)
        record("로컬 편집이 원격 리스에 막힘", ok, f"{t:.0f}초")

        # 4) 원격 MCP 도구로 보내기
        init = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "send_mail", "arguments": {"recipient": "pm", "body_md": "원격 MCP→로컬", "priority": "normal"}}},
        ]
        out = remote_mcp(init, 3)
        tools = next((o["result"]["tools"] for o in out if o.get("id") == 2), [])
        record("원격 MCP 도구 목록", bool(tools), ",".join(t["name"] for t in tools))
        ok, t = until(lambda: has_msg(local_state(), "원격 MCP→로컬"), 20)
        record("원격 MCP send_mail → 로컬 도착", ok, f"{t:.1f}초")

        # 5) 채널 푸시: 원격 MCP가 듣는 동안 로컬에서 긴급/일반을 보냄
        box: dict = {}
        th = threading.Thread(target=lambda: box.update(lines=remote_mcp(init[:2], 25)))
        th.start()
        time.sleep(6)
        local_post("/api/send", {"recipient": "be", "body_md": "푸시 시험 긴급", "priority": "urgent"})
        local_post("/api/send", {"recipient": "be", "body_md": "푸시 시험 일반", "priority": "normal"})
        th.join()
        notes = [o for o in box.get("lines", []) if o.get("method") == "notifications/claude/channel"]
        texts = [n["params"]["content"] for n in notes]
        record("긴급 메시지 → 원격 Claude 채널 푸시", any("푸시 시험 긴급" in x for x in texts), " | ".join(texts)[:90])
        record("일반 메시지도 채널 푸시(설계상 미지원)", any("푸시 시험 일반" in x for x in texts), "")

        # 6) 원격 재시작: 꺼진 동안 보낸 메시지 복구 + 원격 리스 유지
        stop_remote()
        time.sleep(2)
        local_post("/api/send", {"recipient": "be", "body_md": "꺼진 동안 보낸 메시지", "priority": "normal"})
        start_remote(ticket)
        ok, t = until(lambda: has_msg(remote_state(), "꺼진 동안 보낸 메시지"), 45)
        record("원격 재시작 후 놓친 메시지 복구", ok, f"{t:.0f}초")
        ok, _ = until(lambda: any(l["glob_pattern"] == "src/web/*" for l in remote_state()["leases"]), 10)
        record("원격 재시작 후 자기 리스 유지", ok)
    finally:
        stop_remote()
        pm.terminate()
        try:
            pm.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pm.kill()

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n합계: {passed}/{len(results)} 통과  (로컬 로그: {tmp / 'pm.log'}, 원격 로그: {HOST}:{RDIR}/be.log)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
