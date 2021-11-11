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
    group = raft_test.RaftGroup(n)
    await alog.init(INFO)

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
            return

        term, leader = max(group.leaders.items())

        await alog.log(INFO, f"# Successfully elected {leader} for term {term}")

        await alog.log(INFO, f"# Partitioning off leader {leader}, waiting for next one to be elected")
        group.network.set_partition([leader], [str(p) for p in range(n) if str(p) != leader])

        def next_term(group):
            term2, nodes = max(group.normal_op.items())
            return term2 > term and len(nodes) == n-1

        await group.wait_predicate(next_term)

        if len(group.leaders) > 2:
            await alog.log(ERROR, "### Error!  more than 2 terms with a leader!")
            return

        term2, leader2 = max(group.leaders.items())

        if leader2 == leader:
            await alog.log(ERROR, "### Error! Partitioned leader elected!")
            return

        await alog.log(INFO, f"# Successfully elected {leader2} for term {term2}")
        await alog.log(INFO, f"# Repairing partition, waiting for old leader ({leader}) to catch up")

        group.network.repair_partition()

        def full_group(group):
            _, nodes = max(group.normal_op.items())
            return len(nodes) == n

        await group.wait_predicate(full_group)

        if len(group.leaders) > 2:
            await alog.log(ERROR, "### Error! Repairing partition should not result in a new term")
            return 

        await alog.log(INFO, "### Partition test passed!")
        await alog.log(INFO, f"# Sent {group.network.message_count} messages, {group.network.byte_count} bytes")
    finally:
        group.stop_all()
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
    
    
        
