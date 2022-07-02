from .uploading_strategy import UploadingStrategy
import os
from dotenv import load_dotenv
from threading import Thread
from queue import Queue
from azure.eventhub import EventHubProducerClient, EventData
from json import dumps

load_dotenv()

THREADS_NUM = 100


class EventHubStrategy(UploadingStrategy):

    def __init__(self, dataset):
        self.data_queue = Queue()
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=os.getenv('EVENT_HUB_CONN_STR'),
            eventhub_name=os.getenv('EVENT_HUB_NAME'))
        [self.data_queue.put(record) for record in dataset]

    def upload_data(self):
        with self.producer:
            threads = [Thread(target=self.__send, daemon=True) for _ in range(THREADS_NUM)]

            for thread in threads:
                thread.start()
            self.data_queue.join()

    def __send(self):

        event_data_batch = self.producer.create_batch()
        can_add = True
        while not self.data_queue.empty() and can_add:
            try:
                event_data_batch.add(EventData(dumps(self.data_queue.get())))
            except ValueError:
                can_add = False
            else:
                self.data_queue.task_done()

        if event_data_batch.size_in_bytes != 0:
            self.producer.send_batch(event_data_batch)
