from strategy import get_strategy_obj
import os
from dotenv import load_dotenv
import redis
from enum import Enum
import requests

load_dotenv()


class UploadingState(Enum):
    PROCESSING = 'PROCESSING'
    COMPLETED = 'COMPLETED'


class State(Enum):
    LAST_MODIFIED = 'LAST_MODIFIED'
    DATA_LOADING = 'DATA_LOADING'


class DataTransferenceHandler:

    def __init__(self, dataset_endpoint, strategy_name):
        self.strategy_name = strategy_name
        self.dataset_endpoint = dataset_endpoint
        self.dataset_response = requests.get(dataset_endpoint)
        self.uploader = get_strategy_obj(strategy_name,
                                         self.dataset_response.json())

        self.redis_cache = redis.StrictRedis(
            host=os.getenv('REDIS_HOSTNAME'),
            port=os.getenv('REDIS_PORT'),
            password=os.getenv('REDIS_ACCESS_KEY'),
            ssl=True,
            decode_responses=True)

    def write_data(self):
        if self.is_data_loaded():
            return f"Data already uploaded to {self.strategy_name}"
        try:
            self.set_status_of_uploading_strategy(UploadingState.PROCESSING)
            self.uploader.upload_data()
            self.set_status_of_uploading_strategy(UploadingState.COMPLETED)
        except Exception as e:
            print(e)
        else:
            return f'Data writen to {self.strategy_name}!'

    def is_data_loaded(self):
        uploading_state = self.redis_cache.hgetall(self.strategy_name)
        if (uploading_state.get(State.LAST_MODIFIED.name) ==
                self.dataset_response.headers['Last-Modified'] and
                uploading_state.get(State.DATA_LOADING.name) ==
                UploadingState.COMPLETED.name):
            return True
        return False

    def set_status_of_uploading_strategy(self, state: UploadingState):
        strategy_status = {State.LAST_MODIFIED.name: self.dataset_response.headers['Last-Modified'],
                           State.DATA_LOADING.name: state.name}

        self.redis_cache.hset(self.strategy_name, mapping=strategy_status)
