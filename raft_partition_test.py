import framework
import asyncio
import sys
from collections import defaultdict
import alog
from logging import INFO, CRITICAL, DEBUG, ERROR

LEADERS = {}
NORMAL_OP = defaultdict(set)
NORMAL_OP_EVENT = None
NORMAL_OP_THRESHOLD = None
NORMAL_OP_TERM = None

class RaftProcess(framework.Process):
    def __init__(self, *args, **kwargs):
        self.term = None
        super().__init__(*args, **kwargs)

    def update_state(self, var, value, index=None):
        alog.log_no_wait(DEBUG, f"update_state [{self.pid}@{self.term}]: {var}[{index}]={value}")
        if var == "term":
            self.term = int(value)
            return

        if self.term is None:
            return

        if var == "leader" and value is not None:
            leader = str(value)
            if self.term in LEADERS:
                if leader != LEADERS[self.term]:
                    alog.log_no_wait(ERROR, f"### Error! Inconsistent leaders for term {self.term}")
            else:
                LEADERS[self.term] = leader
            NORMAL_OP[self.term].add(self.pid)
            alog.log_no_wait(DEBUG, f"Leaders: {LEADERS}; NORMAL_OP: {NORMAL_OP}")
            if NORMAL_OP_THRESHOLD and self.term > NORMAL_OP_TERM and \
                len(NORMAL_OP[self.term]) >= NORMAL_OP_THRESHOLD:
                NORMAL_OP_EVENT.set()

async def monitor_exceptions(tasks):
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        await alog.log(CRITICAL, f"Got exception {type(e)} {e}")

async def main():
    n = int(sys.argv[1])
    if len(sys.argv) > 2:
        command = sys.argv[2:]
    else:
        command = ["./raft"]
    network = framework.Network()
    global NORMAL_OP_EVENT
    global NORMAL_OP_THRESHOLD
    global NORMAL_OP_TERM 

    NORMAL_OP_EVENT = asyncio.Event()
    NORMAL_OP_THRESHOLD = n
    NORMAL_OP_TERM = 0

    await alog.init(INFO)


    await alog.log(INFO, "# Starting processes, waiting for election")
    processes = await asyncio.gather(*[RaftProcess.create(str(pid), network, *command,
        str(pid), str(n)) for pid in range(n)])
    process_dict = { p.pid : p  for p in processes }

    tasks = [ p.reader_task for p in processes ] + \
        [ p.writer_task for p in processes ]
    asyncio.create_task(monitor_exceptions(tasks))
    
    try:
        await asyncio.wait_for(asyncio.create_task(NORMAL_OP_EVENT.wait()),
            timeout=30)

        if len(LEADERS) > 1:
            await alog.log(ERROR, "### Error!  more than 1 term with a leader despite no failures!")
            await asyncio.sleep(5)
            return

        term, leader = LEADERS.popitem()
        await alog.log(INFO, f"# Successfully elected {leader} for term {term}")
        NORMAL_OP_TERM = term 
        NORMAL_OP_THRESHOLD = n-1
        NORMAL_OP_EVENT = asyncio.Event()

        await alog.log(INFO, f"# Partitioning off leader {leader}, waiting for next one to be elected")
        network.set_partition([leader], [str(p) for p in range(n) if str(p) != leader])

        # allow 30 seconds for election
        await asyncio.wait_for(asyncio.create_task(NORMAL_OP_EVENT.wait()),
            timeout=30)

        if len(LEADERS) > 2:
            await alog.log(ERROR, "### Error!  more than 2 terms with a leader!")
            await asyncio.sleep(5)

            return

        term2, leader2 = max(LEADERS.items())

        await alog.log(INFO, f"# Successfully elected {leader2} for term {term2}")
        await alog.log(INFO, f"# Repairing partition, waiting for leader to catch up")

        NORMAL_OP_THRESHOLD = n
        NORMAL_OP_EVENT = asyncio.Event()

        network.repair_partition()
        # allow 30 seconds for election
        await asyncio.wait_for(asyncio.create_task(NORMAL_OP_EVENT.wait()), timeout=30)

        if len(LEADERS) > 2:
            await alog.log(ERROR, "### Error! Repairing partition should not result in a new term")
            return 

        await alog.log(INFO, "### Partition test passed!")

        for p in processes:
            if p.pid != leader:
                p.stop()

        await alog.log(INFO, f"# Sent {network.message_count} messages, {network.byte_count} bytes")

    except asyncio.TimeoutError:
        await alog.log(ERROR, "## Error! Election did not terminate in 30 seconds!")
    return

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
    
    
        
