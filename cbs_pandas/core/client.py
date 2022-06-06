from cbs_pandas.core.dataset import Dataset


class Client:
    def __init__(self):
        pass

    def get(self, dataset):
        return Dataset.from_identifier(dataset)
