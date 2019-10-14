import time
from functools import partial
from pprint import pprint
from typing import Iterable

from torch import multiprocessing



class Worker(multiprocessing.Process):

    def __init__(self, task_queue, result_queue, task_fun_builder, task_fun_kwargs):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task_fun_builder = task_fun_builder
        self.task_fun_kwargs = task_fun_kwargs


    def run(self):
        proc_name = self.name
        task_fun = self.task_fun_builder(**self.task_fun_kwargs)
        while True:
            task_data = self.task_queue.get()
            if task_data is None:
                # Poison pill means shutdown
                print('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            try:
                result = task_fun(task_data)
            except Exception as e:
                result = None

            self.task_queue.task_done()
            self.result_queue.put(result)

class WorkerPool(object):

    def __init__(self, processes, task_fun_builder,task_fun_kwargs, daemons=True) -> None:
        super().__init__()
        self.n_jobs = processes
        self.task_queue = multiprocessing.JoinableQueue()
        self.results_queue = multiprocessing.Queue()
        self.task_fun_supplier = task_fun_builder
        self.task_fun_kwargs = task_fun_kwargs
        self.daemons = daemons

    def __enter__(self):
        consumers = [Worker(self.task_queue, self.results_queue,self.task_fun_supplier,self.task_fun_kwargs) for i in range(self.n_jobs)]
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

def funfun(x):
    time.sleep(0.5)
    s = 'non-daemon-process: %s: %s' % (str(multiprocessing.current_process()), x)
    return s

def minimal_test_task_fun_builder(some_serializable_arg):
    worker_name = str(multiprocessing.current_process())
    print('use "%s"-variable in task_fun_kwargs in worker: %s to build up task-fun'%(some_serializable_arg,worker_name))

    def task_fun(datum):
        with multiprocessing.Pool(processes=3) as p:
            result = list(p.imap_unordered(funfun,[datum+'-%s-subtask-%d'%(some_serializable_arg,k) for k in range(9)]))
        return {'worker-name':worker_name,'results':result}
    return task_fun

if __name__ == '__main__':

    data = ['task-%d'%x for x in range(3)]
    with WorkerPool(processes=2, task_fun_builder=minimal_test_task_fun_builder,task_fun_kwargs={'some_serializable_arg':'some_arg'}, daemons=False) as p:
        x = list(p.process_unordered(data))
    print(len(x))
    pprint(x)
    assert len(x) == len(data)