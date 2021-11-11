import framework 
import alog
import asyncio
from logging import INFO, ERROR, DEBUG
from collections import defaultdict

class RaftGroup:
    def __init__(self, n):
        # map from terms to leaders of term 
        self.leaders = {}
        # map from term to set of processes that
        # reached normal operation in term, i.e., became
        # a leader or a follower        
        self.normal_op = defaultdict(set)
        self.normal_op_event = None
        self.normal_op_threshold = None
        self.normal_op_term = None
        self.predicate = None
        self.predicate_event = None
        self.n = n
        self.error = asyncio.Event()
        self.network = framework.Network()

    async def start(self, command=["./raft"]):
        await alog.log(INFO, f"# Starting {self.n} processes")
        processes = await asyncio.gather(*[RaftProcess.create(str(pid), self.network, 
            *command, str(pid), str(self.n)) for pid in range(self.n)])
        self.processes = { p.pid : p for p in processes }
        for p in processes:
            p.group = self

        self.tasks = [ p.reader_task for p in processes ] + [ p.writer_task for p in processes ]
    
    async def wait_predicate(self, predicate, timeout=30):
        self.predicate = predicate
        self.predicate_event = asyncio.Event()

        error_task = asyncio.create_task(self.error.wait())
        predicate_task = asyncio.create_task(self.predicate_event.wait())

        try: 
            done, _ = await asyncio.wait(self.tasks + [ error_task, predicate_task ],
                timeout = timeout, return_when=asyncio.FIRST_COMPLETED) 
            if error_task in done:
                raise RuntimeError("Error occurred during test")
            else: 
                error_task.cancel()

            # propagate exceptions
            for t in done:
                if t != predicate_task:
                    await t

        except asyncio.TimeoutError:
            await alog.log(ERROR, "### Error! Predicate not satisfied in {timeout} seconds")
            raise
        finally:
            self.predicate = None
            self.predicate_event = None

    def stop_all(self):
        for p in self.processes.values():
            p.stop()
        self.processes = {}
        self.tasks = []

    def stop(self, pid):
        p = self.processes.pop(pid)
        self.tasks.remove(p.reader_task)
        self.tasks.remove(p.writer_task)
        p.stop()

    def check_predicate(self):
        if self.predicate is not None:
            if self.predicate(self):
                self.predicate_event.set()

    
class RaftProcess(framework.Process):
    def __init__(self, *args, **kwargs):
        self.term = None
        super().__init__(*args, **kwargs)

    def update_state(self, var, value, index=None):
        alog.log_no_wait(DEBUG, f"update_state [{self.pid}@{self.term}]: {var}[{index}]={value}")
        if var == "term":
            self.term = int(value)

        if self.term is None:
            return

        if var == "leader" and value is not None:
            leader = str(value)
            if self.term in self.group.leaders:
                if leader != self.group.leaders[self.term]:
                    alog.log_no_wait(ERROR, f"### Error! Inconsistent leaders for term {self.term}")
                    self.group.error.set()
            else:
                self.group.leaders[self.term] = leader
            self.group.normal_op[self.term].add(self.pid)
            alog.log_no_wait(DEBUG, f"Leaders: {self.group.leaders} normal_op: {self.group.normal_op}")

        self.group.check_predicate()

