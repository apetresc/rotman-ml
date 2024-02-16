from typing import Tuple

import numpy as np
from sklearn import datasets
from sklearn.model_selection import train_test_split

def load_data() -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    X, y = datasets.load_iris(return_X_y=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test

def feature_names():
    return datasets.load_iris().feature_names