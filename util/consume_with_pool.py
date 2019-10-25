import multiprocessing
from typing import Iterable

consumer = None


def init_fun(consumer_supplier):
    global consumer
    consumer = consumer_supplier()


def worker_fun(datum):
    global consumer
    consumer(datum)


def pool_consume(data: Iterable, consumer_supplier, num_processes=4, chunksize=1):
    with multiprocessing.Pool(
        processes=num_processes, initializer=init_fun, initargs=(consumer_supplier,)
    ) as pool:
        list(pool.imap_unordered(worker_fun, data, chunksize=chunksize))
