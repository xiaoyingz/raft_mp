import time
import sys

pid = sys.argv[1]
last = None
print("Starting pinger", file=sys.stderr)

while True:
    print(f"SEND {1-int(pid)} PING {pid}", flush=True)
    line = sys.stdin.readline()
    if line is None:
        break
    print(f"Got {line.strip()}", file=sys.stderr)
    time.sleep(2)

print("Done", file=sys.stderr)

