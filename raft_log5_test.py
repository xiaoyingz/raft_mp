import asyncio
import alog
from logging import INFO, ERROR
import raft_test
import secrets
import raft_election_test

NENTRIES = 5

async def main(n, group):
    term, leader = await raft_election_test.elect_leader(n, group)

    entries = [ secrets.token_urlsafe() for _ in range(NENTRIES) ]
    await alog.log(INFO, f"# Logging {entries}, waiting for it to be logged and committed")
    for entry in entries:
        group.processes[leader].log_entry(entry)
        await asyncio.sleep(0.5)

    def all_committed(group):
        return min(group.commitIndex.values()) == 5

    await group.wait_predicate(all_committed)

    # we only need to check leader logs b/c followers logs will be checked for
    # consistency during commitIndex checks

    log_good = False
    if len(group.logs[leader]) != NENTRIES or not all(i in group.logs[leader] for i in range(1,NENTRIES+1)):
        await alog.log(ERROR, "### Expected leader log to have 5 entries")
    elif not all(t == term for t,_ in group.logs[leader].values()):
        await alog.log(ERROR, f"### Expected leader log to have all entries from term {term}")
    elif set(e for _,e in group.logs[leader].values()) != set(entries):
        await alog.log(ERROR, f"### Leader log contains incorrect entries")
    else:
        log_good = True

    if not log_good:
        await alog.log(ERROR, f"### Leader log: {group.logs[leader]}")
        return
    await alog.log(INFO, f"### Log5 test passed")


if __name__ == "__main__":
    asyncio.run(raft_test.run_test(main))
    
    
        
