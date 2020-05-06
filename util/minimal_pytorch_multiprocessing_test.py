from pprint import pprint
from time import time

import torch

from util.worker_pool import WorkerPool, Task
from torch import multiprocessing

multiprocessing.set_start_method(
    "spawn", force=True
)  # needs to be done to get CUDA working
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def funfun(x):
    # time.sleep(0.5)
    start = time()
    dim = 64
    a = torch.randn(100, dim).to(DEVICE)
    b = torch.randn(200, dim).to(DEVICE)
    c = torch.mm(a, b.t()).cpu()
    s = "non-daemon-process: %s took: %0.2f seconds" % (
        str(multiprocessing.current_process()),
        time() - start,
    )
    return s


class MinimalTask(Task):
    def __init__(self, params_that_where_pickled) -> None:
        super().__init__()
        self.params_that_where_pickled = params_that_where_pickled

    def __enter__(self):
        self.variable_that_is_never_pickled = str(multiprocessing.current_process())
        return self

    def __call__(self, data):
        eid, datum = data
        with multiprocessing.Pool(processes=3) as p:
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
