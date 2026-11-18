"""Starts the fraud agents in one terminal. Do not change this file.

    python3 agents.py                  # 1 anomaly agent + 3 velocity agents
    python3 agents.py --velocity 4     # one velocity agent more

Every agent is a separate process running fraud_agent.py, exactly as if you
had started each one in its own terminal; their lines start with their name
([ano-1], [vel-1], ...). Ctrl+C stops all of them.
"""
import argparse
import signal
import subprocess
import sys
import time
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--velocity", type=int, default=3, help="number of velocity agents (default 3)")
ap.add_argument("--anomaly", type=int, default=1, help="number of anomaly agents (default 1)")
args = ap.parse_args()

agent = Path(__file__).with_name("fraud_agent.py")
procs = []
for model, n in (("anomaly", args.anomaly), ("velocity", args.velocity)):
    for i in range(1, n + 1):
        name = f"{model[:3]}-{i}"
        procs.append(subprocess.Popen([sys.executable, "-u", str(agent), "--model", model, "--name", name]))
print(f"started {args.anomaly} anomaly and {args.velocity} velocity agent(s). Ctrl+C stops all.", flush=True)

try:
    for p in procs:
        p.wait()
except KeyboardInterrupt:
    # In a terminal, Ctrl+C reaches every agent anyway; pass it on in case it did not
    # (e.g. this launcher was stopped by another program), so that every agent leaves
    # its consumer group cleanly.
    time.sleep(1)
    for p in procs:
        if p.poll() is None and sys.platform != "win32":
            p.send_signal(signal.SIGINT)
    deadline = time.time() + 15
    for p in procs:
        try:
            p.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            p.kill()
