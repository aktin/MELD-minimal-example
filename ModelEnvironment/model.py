import numpy as np
import pandas as pd
import ydf
import keras


def predict(dataset: pd.DataFrame):
    model = load_model("model")
    return model.predict(dataset)

def get_query():
    with open("ModelEnvironment/query.sql", "r") as file:
        query = file.read()
        return query

def split_dataset(dataset, test_ratio=0.30):
    """Splits a panda dataframe in two."""
    test_indices = np.random.rand(len(dataset)) < test_ratio
    return dataset[~test_indices], dataset[test_indices]

def train(dataset, predictor: str, features: set[str] =None):
    train_df, test_df = split_dataset(dataset)

    # Specify the model.
    learner = ydf.GradientBoostedTreesLearner(label=predictor)


    # Train the model.
    model = learner.train(train_df)

    return model

def save_model(model):
    model.save("model")

def build_artifact(model: keras.Model, path: str) -> None:
    model.export(filepath=path, format="onnx", export_params=True)

def load_model(path: str) -> keras.Model:
    return keras.layers.TFSMLayer(model, call_endpoint='serving_default')