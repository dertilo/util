from functools import partial
from pprint import pprint

import numpy as np
from sklearn import metrics
from sklearn.metrics import average_precision_score


def ttc(fun, x, y, default=None):
    """
    try to calculate, if failing fall back to default
    """
    try:
        return fun(x, y)
    except:
        return default


def calc_classification_metrics(proba, pred, target, target_names):
    assert all(isinstance(x, np.ndarray) for x in [target, pred, proba])
    assert all(
        [target.dtype == "int64", proba.dtype == "float64", pred.dtype == "int64"]
    )
    clf_report = metrics.classification_report(
        y_true=target,
        y_pred=pred,
        target_names=target_names,
        digits=3,
        output_dict=True,
    )
    averages = {
        k: clf_report.pop(k + " avg") for k in ["micro", "macro", "weighted", "samples"]
    }
    for avg_mode in ["macro", "micro"]:
        averages[avg_mode].update(
            {
                "ROC-AUC": ttc(
                    partial(metrics.roc_auc_score, average=avg_mode), target, proba
                ),
                "PR-AUC": ttc(
                    partial(average_precision_score, average=avg_mode), target, proba
                ),
            }
        )

    scores = {
        "averages": averages,
        "labelwise": calc_labelwise_scores(
            clf_report, proba, pred, target, target_names
        ),
        "accuracy": metrics.accuracy_score(
            target, pred
        ),  # same as f1-micro for single-label classification
    }

    return scores


def calc_labelwise_scores(clf_report, proba, pred, target, target_names):
    labelwise = clf_report

    default = [-1] * (len(target_names))

    def calc_pr_auc_interpolated_labelwise(y_target, y_pred_proba, target_names):
        def calc_interpol_pr_auc(target, pred):
            pre, rec, threshs = metrics.precision_recall_curve(
                target, pred, pos_label=1
            )
            pr_auc = metrics.auc(pre, rec)
            return pr_auc

        return [
            ttc(
                calc_interpol_pr_auc,
                y_target[:, target_names.index(target)],
                y_pred_proba[:, target_names.index(target)],
                -1,
            )
            for target in target_names
        ]

    for metric_name, metric in [
        (
            "ROC-AUC",
            ttc(partial(metrics.roc_auc_score, average=None), target, proba, default),
        ),
        (
            "PR-AUC",
            ttc(partial(average_precision_score, average=None), target, proba, default),
        ),
        (
            "PR-AUC-interp",
            calc_pr_auc_interpolated_labelwise(target, proba, target_names),
        ),
    ]:
        for label, metric_value in zip(target_names, metric):
            labelwise[label][metric_name] = metric_value

    return labelwise


if __name__ == "__main__":
    num_classes = 3
    scores = np.random.rand(100, num_classes)
    y_pred_proba = scores / np.expand_dims(np.sum(scores, axis=1), 1)
    y_pred = np.array(y_pred_proba > 0.5, dtype="int64")
    y_target = np.array(np.random.rand(100, num_classes) > 0.5, dtype="int64")

    pprint(
        calc_classification_metrics(
            y_pred_proba, y_pred, y_target, target_names=["a", "b", "c"]
        )
    )
