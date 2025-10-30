import pandas as pd

from ModelEnvironment import model
from InternalDataLoader import dataloader

def load_query() -> str:
    return model.get_query()

def get_data(query: str) -> pd.DataFrame:
    return dataloader.get_results(query)

def process_data(data: pd.DataFrame):
    return model.predict(data)

def train_model(dataset: pd.DataFrame):
    m = model.train(dataset, predictor="has_fever")
    model.build_artifact(m, "model.onnx")