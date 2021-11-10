import framework
import asyncio
import sys
from collections import defaultdict
import alog
from logging import INFO, CRITICAL, DEBUG, ERROR
import raft_test

async def main():
    n = int(sys.argv[1])
    if len(sys.argv) > 2:
        command = sys.argv[2:]
    else:
        command = ["./raft"]
    
    await alog.init(INFO)
    group = raft_test.RaftGroup(n)

    try:
        await group.start(command)

        await alog.log(INFO, f"Waiting for a leader to be elected")
        await group.wait_for_normal_op(n, 0)

        if len(group.leaders) > 1:
            await alog.log(ERROR, "### Error!  more than 1 term with a leader despite no failures!")
            await alog.log(ERROR, f"Leaders: {group.leaders}")
            return

        term, leader = max(group.leaders.items())

        await alog.log(INFO, f"# Successfully elected {leader} for term {term}")
        await alog.log(INFO, f"### Election test passed")

        await alog.log(INFO, f"# Sent {group.network.message_count} messages, {group.network.byte_count} bytes")
    finally:
        group.stop_all()
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
    
    
        
