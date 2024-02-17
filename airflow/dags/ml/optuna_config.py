"""<>"""
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestRegressor
from pandas import DataFrame
from optuna import Trial

import numpy as np

def objective(trial : Trial, x : DataFrame, y : np.ndarray) -> float:
    """<>"""
    n_estimators = trial.suggest_int('n_estimators', 2, 20)
    max_depth = int(trial.suggest_float('max_depth', 1, 32, log=True))

    clf = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth)

    return cross_val_score(clf, x, y, n_jobs=-1, cv=3).mean()
