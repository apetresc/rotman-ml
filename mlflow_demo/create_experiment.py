import mlflow
from mlflow.models import infer_signature

mlflow.set_tracking_uri(uri="https://mlflow.apetre.sc")

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

import common


X_train, X_test, y_train, y_test = common.load_data()

params = {
    "solver": "lbfgs",
    "max_iter": 1000,
    "multi_class": "auto",
    "random_state": 8888
}

lr = LogisticRegression(**params)
lr.fit(X_train, y_train)

y_pred = lr.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)



mlflow.set_experiment("MLFlow Quickstart")

with mlflow.start_run():
    # Log the hyperparameters
    mlflow.log_params(params)

    mlflow.log_metric("accuracy", accuracy)

    mlflow.set_tag("Training Info", "Basic LR model for iris data")

    # Infer the model signature
    signature = infer_signature(X_train, lr.predict(X_train))

    # Log the model
    model_info = mlflow.sklearn.log_model(
        sk_model=lr,
        artifact_path="iris_model",
        signature=signature,
        input_example=X_train,
        registered_model_name="training-quickstart"
    )
