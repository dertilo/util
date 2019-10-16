import time
import traceback
from abc import abstractmethod
from pprint import pprint
from typing import Iterable, NamedTuple, Any
from torch import multiprocessing
multiprocessing.set_start_method('spawn', force=True)  # needs to be done to get CUDA working
# multiprocessing = None

class Task(object):
    # def __init__(self):
        # global multiprocessing #TODO: the Task cannot influence the Worker
        # if multiprocessing is None:
        #     if i_want_to_use_torch_cuda:
        #         from torch import multiprocessing
        #         multiprocessing.set_start_method('spawn', force=True)  # needs to be done to get CUDA working
        #     else:
        #         import multiprocessing

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def __call__(self, data):
        raise NotImplementedError

class Worker(multiprocessing.Process):

    def __init__(self, task_queue, result_queue, task:Task):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task:Task = task

    def run(self):
        proc_name = self.name
        with self.task as task:
            while True:
                task_data = self.task_queue.get()
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
                if isinstance(task_data,IdWork):
                    putit = (task_data.eid,result)
                else:
                    putit = result
                self.result_queue.put(putit)

class IdWork(NamedTuple):
    eid:int
    work:Any

class WorkerPool(object):

    def __init__(self, processes,task:Task, daemons=True) -> None:
        super().__init__()
        self.n_jobs = processes
        self.task_queue = multiprocessing.JoinableQueue()
        self.results_queue = multiprocessing.Queue()
        self.task = task
        self.daemons = daemons

    def __enter__(self):
        consumers = [Worker(self.task_queue,
                            self.results_queue,
                            self.task) for i in range(self.n_jobs)]
        for w in consumers:
            w.daemon = self.daemons
            w.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for i in range(self.n_jobs):
            self.task_queue.put(None)
        self.task_queue.join()
        self.results_queue.close()

    def process_unordered(self, data_g:Iterable):
        data_iter = iter(data_g)
        [self.task_queue.put(next(data_iter)) for i in range(self.n_jobs)]
        for datum in data_iter:
            yield self.results_queue.get()
            self.task_queue.put(datum)

        for i in range(self.n_jobs):
            yield self.results_queue.get()

    def process(self,data):
        eided_data = [IdWork(id(d),d) for d in data]
        id2result = {task_id:r for task_id,r in self.process_unordered(eided_data)}
        return [id2result[e.eid] for e in eided_data]

#######################################################################################################
def funfun(x):
    time.sleep(0.5)
    s = 'non-daemon-process: %s: %s' % (str(multiprocessing.current_process()), x)
    return s

class MinimalTask(Task):

    def __init__(self,params_that_where_pickled) -> None:
        super().__init__()
        self.params_that_where_pickled = params_that_where_pickled

    def __enter__(self):
        self.variable_that_is_never_pickled = str(multiprocessing.current_process())
        return self

    def __call__(self, data):
        with multiprocessing.Pool(processes=3) as p:
            result = list(
                p.imap_unordered(funfun, [data + '-%s-subtask-%d' % (self.params_that_where_pickled, k) for k in range(9)]))
        return {'worker-name': self.variable_that_is_never_pickled, 'results': result}


if __name__ == '__main__':

    data = ['task-%d'%x for x in range(3)]
    with WorkerPool(processes=2,
                    task = MinimalTask('some-param'),
                    daemons=False) as p:
        x = list(p.process_unordered(data))
    print(len(x))
    pprint(x)
    assert len(x) == len(data)