import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ModelManager import manager

if __name__ == "__main__":
    data = pd.DataFrame([36., 37., 38., 39., 35.], columns=["temperature_celsius"])
    predictions = manager.process_data(data)

    print(predictions)