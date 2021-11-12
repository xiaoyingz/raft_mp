import framework
import asyncio
import sys
from collections import defaultdict
import alog
from logging import INFO, CRITICAL, DEBUG, ERROR
import raft_test
import secrets

async def main():
    n = int(sys.argv[1])
    if len(sys.argv) > 2:
        command = sys.argv[2:]
    else:
        command = ["./raft"]
    
    await alog.init(DEBUG)
    group = raft_test.RaftGroup(n)

    try:
        await group.start(command)

        await alog.log(INFO, f"Waiting for a leader to be elected")
        def reached_normal_op(group):
            if not group.normal_op: # no leaders yet
                return False
            _, nodes = max(group.normal_op.items())
            if len(nodes) == n:
                return True
            else:
                return False
        await group.wait_predicate(reached_normal_op)

        if len(group.leaders) > 1:
            await alog.log(ERROR, "### Error!  more than 1 term with a leader despite no failures!")
            await alog.log(ERROR, f"Leaders: {group.leaders}")
            return

        term, leader = max(group.leaders.items())

        await alog.log(INFO, f"# Successfully elected {leader} for term {term}")

        entry = secrets.token_urlsafe()
        await alog.log(INFO, f"# Logging {entry}, waiting for it to be logged and committed")
        group.processes[leader].log_entry(entry)

        def all_committed(group):
            return min(group.commitIndex.values()) == 1

        await group.wait_predicate(all_committed)

        for p in map(str,range(n)):
            if len(group.logs[p]) != 1 or 1 not in group.logs[p] or \
                group.logs[p][1] != [term,entry]:
                await alog.log(ERROR, f"### Incorrect log for {p}: {group.logs[p]}")
                await alog.log(ERROR, f"### Expected {{ 1 : {repr([term,entry])} }}")
                return

        await alog.log(INFO, f"### Simple log test passed")

        await alog.log(INFO, f"# Sent {group.network.message_count} messages, {group.network.byte_count} bytes")
    finally:
        group.stop_all()
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
    
    
        
