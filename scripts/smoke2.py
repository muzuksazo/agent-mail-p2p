#!/usr/bin/env python3
"""한 컴퓨터에서 노드 2개(pm, be)를 띄워 P2P 동작을 확인하는 스모크 시험.

확인 항목
  1. 합류 전에 pm이 보낸 메시지가 be에 복구되는가
  2. 합류 전에 pm이 건 리스가 be에 복구되는가
  3. 합류 전에 pm이 올린 팀원 정보(Loro)가 be에 보이는가
  4. 합류 후 메시지가 양방향으로 전파되는가

사용법: python3 scripts/smoke2.py [실행 파일 경로]  (기본: target/release/agent-mail-p2p)
임시 폴더와 18775/18776 포트를 쓰며, 끝나면 자기가 띄운 프로세스만 종료한다.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

BIN = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).resolve().parent.parent / "target/release/agent-mail-p2p")
PM_PORT, BE_PORT = 18775, 18776
WAIT_S = int(os.environ.get("SMOKE_WAIT_S", "45"))


def state(port: int) -> dict:
    return json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/state", timeout=3))


def wait_http(port: int, timeout: float = 20) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            state(port)
            return
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"port {port} did not come up")


def mcp_calls(agent: str, port: int, calls: list[tuple[str, dict]]) -> None:
    p = subprocess.Popen([BIN, "mcp", "--agent", agent, "--daemon-url", f"http://127.0.0.1:{port}", "--no-channel-push"],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)

    def rpc(i: int, method: str, params: dict) -> dict:
        p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": i, "method": method, "params": params}) + "\n")
        p.stdin.flush()
        while True:
            d = json.loads(p.stdout.readline())
            if d.get("id") == i:
                return d

    rpc(1, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "smoke", "version": "0"}})
    p.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
    p.stdin.flush()
    for i, (name, args) in enumerate(calls, start=2):
        r = rpc(i, "tools/call", {"name": name, "arguments": args})
        if "error" in r:
            raise RuntimeError(f"{name} failed: {r['error']}")
    p.stdin.close()
    p.terminate()


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="amp2p-smoke-"))
    procs: list[subprocess.Popen] = []
    logs = {"pm": tmp / "pm.log", "be": tmp / "be.log"}
    results: list[tuple[str, bool, str]] = []
    try:
        pm = subprocess.Popen([BIN, "open", "--agent", "pm", "--name", "PM", "--port", str(PM_PORT), "--data-dir", str(tmp / "pm")],
                              stdout=subprocess.DEVNULL, stderr=open(logs["pm"], "w"))
        procs.append(pm)
        wait_http(PM_PORT)
        ticket = ""
        for _ in range(50):
            m = re.search(r"amp2p_[a-z0-9]+", logs["pm"].read_text())
            if m:
                ticket = m.group(0)
                break
            time.sleep(0.2)
        if not ticket:
            raise RuntimeError("no ticket printed")

        mcp_calls("pm", PM_PORT, [
            ("send_mail", {"recipient": "all", "body_md": "합류 전 메시지", "priority": "normal"}),
            ("reserve_files", {"glob_pattern": "docs/*"}),
        ])

        be = subprocess.Popen([BIN, "join", ticket, "--agent", "be", "--name", "BE", "--port", str(BE_PORT), "--data-dir", str(tmp / "be")],
                              stdout=subprocess.DEVNULL, stderr=open(logs["be"], "w"))
        procs.append(be)
        wait_http(BE_PORT)

        start = time.time()
        b = state(BE_PORT)
        while time.time() - start < WAIT_S:
            b = state(BE_PORT)
            has_msg = any(m.get("body_md") == "합류 전 메시지" for m in b["messages"])
            has_lease = any(l["glob_pattern"] == "docs/*" for l in b["leases"])
            has_pm = any(a["id"] == "pm" for a in b["agents"])
            if has_msg and has_lease and has_pm:
                break
            time.sleep(1)
        took = f"{time.time() - start:.0f}초"
        results.append(("합류 전 메시지 복구", any(m.get("body_md") == "합류 전 메시지" for m in b["messages"]), took))
        results.append(("합류 전 리스 복구", any(l["glob_pattern"] == "docs/*" for l in b["leases"]), took))
        results.append(("합류 전 팀원 정보 복구", any(a["id"] == "pm" for a in b["agents"]), took))

        mcp_calls("pm", PM_PORT, [("send_mail", {"recipient": "be", "body_md": "합류 후 pm→be", "priority": "normal"})])
        mcp_calls("be", BE_PORT, [("send_mail", {"recipient": "pm", "body_md": "합류 후 be→pm", "priority": "normal"})])
        ok_pb = ok_bp = False
        end = time.time() + 15
        while time.time() < end and not (ok_pb and ok_bp):
            ok_pb = any(m.get("body_md") == "합류 후 pm→be" for m in state(BE_PORT)["messages"])
            ok_bp = any(m.get("body_md") == "합류 후 be→pm" for m in state(PM_PORT)["messages"])
            time.sleep(0.5)
        results.append(("합류 후 pm→be 메시지", ok_pb, ""))
        results.append(("합류 후 be→pm 메시지", ok_bp, ""))
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    for name, ok, note in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name}  {note}")
    if os.environ.get("SMOKE_SHOW_LOGS"):
        for who, path in logs.items():
            print(f"--- {who} log")
            print("\n".join(l for l in path.read_text().splitlines() if "DIAG" in l or "rror" in l))
    print(f"(임시 폴더: {tmp})")
    return 0 if results and all(ok for _, ok, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main())
