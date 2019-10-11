import time
from functools import partial
from pprint import pprint

from torch import multiprocessing



class Worker(multiprocessing.Process):

    def __init__(self, task_queue, result_queue,task_fun_supplier):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task_fun_supplier = task_fun_supplier

    def run(self):
        proc_name = self.name
        task_fun = self.task_fun_supplier()
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

    def __init__(self, processes,task_fun_supplier,daemons=True) -> None:
        super().__init__()
        self.n_jobs = processes
        self.task_queue = multiprocessing.JoinableQueue()
        self.results_queue = multiprocessing.Queue()
        self.task_fun_supplier = task_fun_supplier
        self.daemons = daemons


    def __enter__(self):
        consumers = [Worker(self.task_queue, self.results_queue,self.task_fun_supplier) for i in range(self.n_jobs)]
        for w in consumers:
            w.daemon = self.daemons
            w.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for i in range(self.n_jobs):
            self.task_queue.put(None)
        self.task_queue.join()
        self.results_queue.close()

    def imap_unordered(self,data_g):
        data_iter = iter(data_g)
        [self.task_queue.put(next(data_iter)) for i in range(self.n_jobs)]
        for datum in data_iter:
            yield self.results_queue.get()
            self.task_queue.put(datum)

        for i in range(self.n_jobs):
            yield self.results_queue.get()

def funfun(x):
    time.sleep(0.5)
    s = '%s: %s' % (str(multiprocessing.current_process()), x)
    return s

global_data = None

def minimal_test_task_fun_supplier():
    global global_data
    if global_data is None:
        global_data = str(multiprocessing.current_process())

    def fun(datum):
        with multiprocessing.Pool(processes=2) as p:
            result = list(p.imap_unordered(funfun,[datum+'-subtask-%d'%k for k in range(9)]))
        return {'global_data':global_data,'results':result}
    return fun

if __name__ == '__main__':

    data = ['task-%d'%x for x in range(3)]
    with WorkerPool(processes=2, task_fun_supplier=minimal_test_task_fun_supplier, daemons=False) as p:
        x = list(p.imap_unordered( data))
    print(len(x))
    pprint(x)
    assert len(x) == len(data)