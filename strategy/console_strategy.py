from time import sleep
from .uploading_strategy import UploadingStrategy


class ConsoleStrategy(UploadingStrategy):

    def __init__(self, dataset):
        self.dataset = dataset

    def upload_data(self):
        for record in self.dataset:
            print(record)
            sleep(0.1)
