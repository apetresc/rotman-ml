from typing import cast

import mlflow
import mlflow.pyfunc
from mlflow.entities import Run
import pandas as pd

import common


mlflow.set_tracking_uri(uri="https://mlflow.apetre.sc")

experiment = mlflow.get_experiment_by_name("MLFlow Quickstart")
if not experiment:
    raise RuntimeError("No experiment found")

runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], output_format="list")
if not runs:
    raise RuntimeError("No runs found")

most_recent_run = cast(Run, runs[0])
print("Most recent run:", most_recent_run)

model_name = "training-quickstart"
model_version = 1
model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{model_version}")
print("Model:", model)

X_train, X_test, y_train, y_test = common.load_data()
predictions = model.predict(X_test)
result = pd.DataFrame(X_test, columns=common.feature_names())
result["actual_class"] = y_test
result["predicted_class"] = predictions

print(result[:10])
