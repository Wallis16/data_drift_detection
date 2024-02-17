"""<>"""
from data_drift_analysis.analysis import retrieve_from_mongodb
from ml.optuna_config import objective
from sklearn.model_selection import train_test_split

import optuna
import pandas as pd

numerical_features = ['passenger_count', 'trip_distance',
                           'payment_type', 'fare_amount']

def skelarn_model(username: str, password: str, database: str
                  , collection_name: str, reference_day: int, current_day: int):
    """<>"""

    reference = retrieve_from_mongodb(username, password, database
                                      , collection_name+'_'+str(reference_day))
    reference = reference[numerical_features]

    current = retrieve_from_mongodb(username, password, database,
                                     collection_name+'_'+str(current_day))
    current = current[numerical_features]

    all_data = pd.concat([reference, current]).dropna()

    x, y = all_data[['passenger_count', 'trip_distance',
                      'payment_type']], all_data[['fare_amount']]

    x_train, _, y_train, _ = train_test_split(x, y.values.ravel(),
                                    test_size=0.33, random_state=42)
    study = optuna.create_study(study_name='optimization', direction='maximize')
    study.optimize(lambda trial: objective(trial, x_train, y_train), n_trials=10)

    best_trial = study.best_trial

    print(f'Accuracy: {best_trial.value}')
    print(f'Best hyperparameters: {best_trial.params}')
