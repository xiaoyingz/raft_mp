import asyncio
import sys
import raft_test
import alog
from logging import INFO, ERROR


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
            await alog.log(ERROR, f"leaders: {group.leaders}")
            return


        term, leader = max(group.leaders.items())
        await alog.log(INFO, f"# Successfully elected {leader} for term {term}")

        await alog.log(INFO, f"# Stopping leader {leader}, waiting for next one to be elected")

        group.stop(leader)

        def next_term(group):
            term2, nodes = max(group.normal_op.items())
            return term2 > term and len(nodes) == n-1

        await group.wait_predicate(next_term)

        if len(group.leaders) > 2:
            await alog.log(ERROR, "### Error!  more than 2 terms with a leader!")
            await alog.log(ERROR, f"leaders: {group.leaders}")
            return

        term2, leader2 = max(group.leaders.items())

        if leader2 == leader:
            await alog.log(ERROR, "### Error! new leader cannot be stopped process!")
            return

        await alog.log(INFO, f"# Successfully elected {leader2} for term {term2}")
        await alog.log(INFO, "### Election after failure test passed!")
        await alog.log(INFO, f"# Sent {group.network.message_count} messages, {group.network.byte_count} bytes")

    finally:
        group.stop_all()
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())


    
    
        
