import hashlib
import os
import subprocess
from time import time
from typing import Iterable, Generator, List, Dict, Any, TypeVar, Callable

import numpy as np
from scipy.sparse import csr_matrix, vstack


def insert_or_append(d, k, v):
    if k in d:
        d[k].append(v)
    else:
        d[k] = [v]


def get_dict_paths(paths, root_path, my_dict):
    if not isinstance(my_dict, dict):
        paths.append(root_path)
        return root_path
    for k, v in my_dict.items():
        path = root_path + [k]
        get_dict_paths(paths, path, v)


def get_val(d, path):
    for p in path:
        d = d.get(p)
    return d


def set_val(d, path, value):
    for i in range(len(path) - 1):
        p = path[i]
        if p in d:
            d = d.get(p)
        else:
            d.__setitem__(p, {})
            d = d.get(p)
    d.__setitem__(path[-1], value)


def exec_command(command):
    p = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return {"stdout": p.stdout.readlines(), "stderr": p.stderr.readlines()}


def dicts_to_csr(dicts: List[Dict], num_rows=None, num_cols=None):
    assert isinstance(dicts, list) and len(dicts) > 0
    assert all([isinstance(d, dict) for d in dicts])
    g = [(row, int(col), val) for row, d in enumerate(dicts) for col, val in d.items()]
    col = [t[1] for t in g]
    values = [t[2] for t in g]
    row = [t[0] for t in g]
    num_dim = max(col) + 1
    shape = (
        num_rows if num_rows is not None else len(dicts),
        num_cols if num_cols is not None else num_dim,
    )
    return csr_matrix((values, (row, col)), shape=shape)


def merge_dicts(dicts: Iterable):

    result = {}
    for dictionary in dicts:
        result.update(dictionary)
    return result


def csr_vectors_to_dicts(vects: List[csr_matrix]):
    csr = vstack(vects, format="csr")
    return csr_to_dicts(csr)


def ndarray_to_dicts(x: np.ndarray, dim_names=None, filter_on_val=lambda v: True):
    if dim_names is None:
        dim_names = [i for i in range(x.shape[1])]
    return [
        {dim_names[col]: val for col, val in enumerate(row) if filter_on_val(val)}
        for row in x
    ]


def csr_to_dicts(x: csr_matrix, dim_names=None):
    if dim_names is None:
        dim_names = [i for i in range(x.shape[1])]
    vert_idx, horiz_idx = x.nonzero()
    return [
        {
            dim_names[k]: v
            for k, v in zip(
                horiz_idx[np.where(vert_idx == row_idx)],
                x.data[np.where(vert_idx == row_idx)],
            )
        }
        for row_idx in range(x.shape[0])
    ]


def process_batchwise(process_fun, iterable: Iterable, batch_size=1024):
    return (
        d
        for batch in iterable_to_batches(iterable, batch_size)
        for d in process_fun(batch)
    )


def consume_batchwise(consume_fun, iterable: Iterable, batch_size=1024):
    for batch in iterable_to_batches(iterable, batch_size):
        consume_fun(batch)


T = TypeVar("T")


def iterable_to_batches(
    g: Iterable[T], batch_size: int
) -> Generator[List[T], None, None]:
    g = iter(g) if isinstance(g, list) else g
    batch = []
    while True:
        try:
            batch.append(next(g))
            if len(batch) == batch_size:
                yield batch
                batch = []
        except StopIteration as e:  # there is no next element in iterator
            break
    if len(batch) > 0:
        yield batch


def hash_list_of_strings(l: List[str]):
    return hashlib.sha1("_".join(l).encode("utf-8")).hexdigest()


def iterate_and_time(g):
    while True:
        start = time()
        try:
            d = next(g)
        except StopIteration:
            break
        yield d, time() - start


import concurrent.futures as cf


def process_with_threadpool(data: List[Dict], process_fun: Callable, max_workers=1):
    """see: https://docs.python.org/3/library/concurrent.futures.html"""
    with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sample = [executor.submit(process_fun, **d) for d in data]
        for future in cf.as_completed(future_to_sample):
            yield future.result()


if __name__ == "__main__":
    start = time()

    def sleep(k):
        os.system("sleep 1")
        return k

    data = [{"k": k} for k in range(10)]
    print(list(process_with_threadpool(data, sleep, 10)))
    print(
        "concurrently sleeping %d times in took %0.2f seconds"
        % (len(data), time() - start)
    )
