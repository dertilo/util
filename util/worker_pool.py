import time
import traceback
from abc import abstractmethod
from pprint import pprint
from typing import Iterable, NamedTuple, Any, Dict

try:
    from torch import multiprocessing as mp

    mp.set_start_method("spawn", force=True)  # needs to be done to get CUDA working
    # multiprocessing = None

except ImportError:  # TODO: this is shitty
    import multiprocessing as mp


class Task(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def __call__(self, data):
        raise NotImplementedError


class GenericTask(Task):
    def __init__(self, **kwargs) -> None:
        self.task_params = (
            kwargs  # these get pickled and send over multiprocessing.Queue
        )
        super().__init__()

    def __enter__(self):
        self.task_data = self.build_task_data(**self.task_params)
        return self

    def __call__(self, job):
        return self.process(job, self.task_data)

    @staticmethod
    def build_task_data(**task_params):
        """
        only called once in Worker to setup/start the task
        :param task_params:
        """
        pass

    @classmethod
    @abstractmethod
    def process(cls, job, task_data):
        """
        :param job gets send over multiprocessing.Queue
        :return gets send over multiprocessing.Queue
        """
        raise NotImplementedError


class Worker(mp.Process):
    def __init__(
        self, task_queue: mp.JoinableQueue, result_queue: mp.Queue, task: Task
    ):
        mp.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task: Task = task

    def run(self):
        proc_name = self.name
        with self.task as task:
            while True:
                stuff = self.task_queue.get()
                if isinstance(stuff, IdWork):
                    work_id, task_data = stuff.eid, stuff.work
                else:
                    work_id, task_data = None, stuff

                if task_data is None:
                    # Poison pill means shutdown
                    self.task_queue.task_done()
                    break
                try:
                    result = task(task_data)
                except Exception as e:
                    traceback.print_exc()
                    result = None

                self.task_queue.task_done()
                if work_id is not None:
                    putit = (work_id, result)
                else:
                    putit = result
                self.result_queue.put(putit)


class IdWork(NamedTuple):
    eid: int
    work: Any


class WorkerPool(object):
    def __init__(self, processes: int, task: Task, daemons=True) -> None:
        super().__init__()
        self.num_workers = processes
        self.task_queue = mp.JoinableQueue()
        self.results_queue = mp.Queue()
        self.task = task
        self.daemons = daemons

    def __enter__(self):
        consumers = [
            Worker(self.task_queue, self.results_queue, self.task)
            for i in range(self.num_workers)
        ]
        for w in consumers:
            w.daemon = self.daemons
            w.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for i in range(self.num_workers):
            self.task_queue.put(None)
        self.task_queue.join()
        self.results_queue.close()

    def process_unordered(self, data_g: Iterable):
        data_iter = iter(data_g)
        [self.task_queue.put(next(data_iter)) for i in range(self.num_workers)]
        for datum in data_iter:
            yield self.results_queue.get()
            self.task_queue.put(datum)

        for i in range(self.num_workers):
            yield self.results_queue.get()

    def process(self, data):
        eided_data = [IdWork(k, d) for k, d in enumerate(data)]
        assert len(set(w.eid for w in eided_data)) == len(data)
        id2result = {task_id: r for task_id, r in self.process_unordered(eided_data)}
        return [id2result[e.eid] for e in eided_data]


#######################################################################################################
def funfun(x):
    # time.sleep(0.5)
    s = "non-daemon-process: %s: %s" % (str(mp.current_process()), x)
    return s


class MinimalTask(Task):
    def __init__(self, params_that_where_pickled) -> None:
        super().__init__()
        self.params_that_where_pickled = params_that_where_pickled

    def __enter__(self):
        self.variable_that_is_never_pickled = str(mp.current_process())
        return self

    def __call__(self, data):
        eid, datum = data
        time.sleep(datum)
        with mp.Pool(processes=3) as p:
            result = list(
                p.imap_unordered(
                    funfun,
                    [
                        "task-%d-%s-subtask-%d"
                        % (eid, self.params_that_where_pickled, k)
                        for k in range(9)
                    ],
                )
            )
        return {"worker-name": self.variable_that_is_never_pickled, "results": result}


if __name__ == "__main__":

    data = [(x, 3 - x) for x in range(3)]
    with WorkerPool(processes=2, task=MinimalTask("some-param"), daemons=False) as p:
        x = p.process(data)
    print(len(x))
    pprint(x)
    assert len(x) == len(data)
