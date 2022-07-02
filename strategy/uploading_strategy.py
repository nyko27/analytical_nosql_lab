from abc import ABC, abstractmethod


class UploadingStrategy(ABC):

    @abstractmethod
    def upload_data(self):
        pass
