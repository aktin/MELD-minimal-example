import numpy as np
import pandas as pd
import tensorflow_decision_forests as tfdf
import tf_keras


def predict(dataset: pd.DataFrame):
    model = load_model()
    dataset = tfdf.keras.pd_dataframe_to_tf_dataset(dataset)
    return model.predict(dataset)

def get_query():
    with open("ModelEnvironment/query.sql", "r") as file:
        query = file.read()
        return query

def split_dataset(dataset, test_ratio=0.30):
    """Splits a panda dataframe in two."""
    test_indices = np.random.rand(len(dataset)) < test_ratio
    return dataset[~test_indices], dataset[test_indices]

def train(dataset, predictor, features=None):
    train_ds_pd, test_ds_pd = split_dataset(dataset)

    train_ds = tfdf.keras.pd_dataframe_to_tf_dataset(train_ds_pd, label=predictor)
    test_ds = tfdf.keras.pd_dataframe_to_tf_dataset(test_ds_pd, label=predictor)

    # Specify the model.
    model = tfdf.keras.RandomForestModel(verbose=2)

    # Train the model.
    model.fit(train_ds)

    model.compile(metrics=["accuracy"])

    _, accuracy = model.evaluate(test_ds, verbose=0)
    print(f"Test accuracy: {round(accuracy * 100, 2)}%")

    print(model.summary())

    return model

def save_model(model):
    model.save("model")

def load_model():
    return tf_keras.models.load_model("model")