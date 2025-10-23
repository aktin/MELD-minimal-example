from ModelManager import manager

query = manager.load_query()

data = manager.get_data(query)
manager.train_model(data)

