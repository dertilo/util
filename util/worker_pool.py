import time
import traceback
from functools import partial
from pprint import pprint
from typing import Iterable

from torch import multiprocessing



class Worker(multiprocessing.Process):

    def __init__(self, task_queue, result_queue,task_fun , task_fun_kwargs_supplier):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task_fun = task_fun
        self.task_fun_kwargs_supplier = task_fun_kwargs_supplier


    def run(self):
        proc_name = self.name

        def partial_fun_builder(task_fun, task_fun_kwargs_supplier):
            kwargs = task_fun_kwargs_supplier()

            def fun(datum):
                return task_fun(datum, **kwargs)

            return fun

        partial_fun = partial_fun_builder(self.task_fun,self.task_fun_kwargs_supplier)
        while True:
            task_data = self.task_queue.get()
            if task_data is None:
                # Poison pill means shutdown
                # print('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            try:
                result = partial_fun(task_data)
            except Exception as e:
                traceback.print_exc()
                result = None

            self.task_queue.task_done()
            self.result_queue.put(result)

class WorkerPool(object):

    def __init__(self, processes, task_fun, task_fun_kwargs_supplier, daemons=True) -> None:
        super().__init__()
        self.n_jobs = processes
        self.task_queue = multiprocessing.JoinableQueue()
        self.results_queue = multiprocessing.Queue()
        self.task_fun = task_fun
        self.task_fun_kwargs_supplier = task_fun_kwargs_supplier
        self.daemons = daemons

    def __enter__(self):
        consumers = [Worker(self.task_queue, self.results_queue,self.task_fun,self.task_fun_kwargs_supplier) for i in range(self.n_jobs)]
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

#######################################################################################################
def funfun(x):
    time.sleep(0.5)
    s = 'non-daemon-process: %s: %s' % (str(multiprocessing.current_process()), x)
    return s


def minimal_task_kwargs_supplier():
    worker_name = str(multiprocessing.current_process())
    kwargs = {'some_serializable_arg': 'some_arg','worker_name':worker_name}
    print('supplying kwargs "%s" for worker: %s'%(str(kwargs),worker_name))
    return kwargs

def minimal_task_fun(datum,some_serializable_arg,worker_name):
    with multiprocessing.Pool(processes=3) as p:
        result = list(p.imap_unordered(funfun,[datum+'-%s-subtask-%d'%(some_serializable_arg,k) for k in range(9)]))
    return {'worker-name':worker_name,'results':result}

if __name__ == '__main__':

    data = ['task-%d'%x for x in range(3)]
    with WorkerPool(processes=2, task_fun=minimal_task_fun, task_fun_kwargs_supplier=minimal_task_kwargs_supplier, daemons=False) as p:
        x = list(p.process_unordered(data))
    print(len(x))
    pprint(x)
    assert len(x) == len(data)