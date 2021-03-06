import random
from pprint import pprint
from typing import Iterable, List, Any
from util.worker_pool import WorkerPool, Task

import numpy as np

from util.util_methods import get_dict_paths, set_val, get_val
import warnings

warnings.filterwarnings("ignore")


def imputed(x):
    return np.NaN if isinstance(x, str) else x


def calc_mean_and_std(eval_metrices):
    assert isinstance(eval_metrices, list) and all(
        [isinstance(d, dict) for d in eval_metrices]
    )
    paths = []
    get_dict_paths(paths, [], eval_metrices[0])
    means = {}
    stds = {}
    for p in paths:
        try:
            m_val = np.mean([imputed(get_val(d, p)) for d in eval_metrices])
            set_val(means, p, m_val)
        except:
            print(p)
        try:
            std_val = np.std([imputed(get_val(d, p)) for d in eval_metrices])
            set_val(stds, p, std_val)
        except:
            print(p)

    return means, stds


def calc_mean_std_scores(score_task: Task, scoring_jobs, n_jobs=0):
    scores = calc_scores(score_task, scoring_jobs, n_jobs)
    assert len(scores) == len(scoring_jobs)

    m_scores, std_scores = calc_mean_and_std(scores)
    return {"m_scores": m_scores, "std_scores": std_scores}


def calc_scores(score_task: Task, scoring_jobs: List[Any], n_jobs):
    if n_jobs > 0:
        with WorkerPool(processes=n_jobs, task=score_task, daemons=False) as p:
            scores = p.process(scoring_jobs)
    else:
        with score_task as task:
            scores = [task(job) for job in scoring_jobs]
    assert len(scores) == len(scoring_jobs)
    assert all(s is not None for s in scores)
    return scores


class ScoreTask(Task):
    def __init__(self, score_fun, build_kwargs_fun, builder_kwargs) -> None:
        super().__init__()
        self.score_fun = score_fun
        self.build_kwargs_fun = build_kwargs_fun
        self.builder_kwargs = builder_kwargs

    def __enter__(self):
        self.kwargs = self.build_kwargs_fun(**self.builder_kwargs)
        return self

    def __call__(self, data):
        return self.score_fun(data, **self.kwargs)


"""
minimal example
"""


def example_build_kwargs_fun(param):
    return {"model_data": "some-model-%s" % param}


def example_score_fun(split, model_data):
    return {
        model_data: {
            "dummy-score": {split_name: random.random() for split_name in split},
        }
    }


if __name__ == "__main__":

    task = ScoreTask(
        example_score_fun, example_build_kwargs_fun, {"param": "testparam"}
    )
    scores = calc_scores(
        task, scoring_jobs=[("train", "test") for k in range(3)], n_jobs=2
    )
    pprint(scores)

    mscores = calc_mean_std_scores(
        task, scoring_jobs=[("train", "test") for k in range(3)], n_jobs=2
    )
    pprint(mscores)
