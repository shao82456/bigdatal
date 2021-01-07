import _thread
import json
import random
import threading
import time
from typing import List

from kafka import KafkaProducer

from cep.BatchInfo import BatchInfo, MyEncoder

APP_ID = "app_123"
BATCH_TIME = 10
COMPUTE_MAX = 20
lock = threading.Lock()
batches: List[BatchInfo] = []
producer: KafkaProducer = KafkaProducer(bootstrap_servers="localhost:9092")


def submit_job():
    while True:
        newBatch = BatchInfo(APP_ID)
        lock.acquire()
        newBatch.submissionTime = int(time.time())
        batches.append(newBatch)
        lock.release()
        send(newBatch)
        print("submitted: %d" % newBatch.submissionTime)
        time.sleep(10)


def compute_job():
    while True:
        lock.acquire()
        newBatch: BatchInfo = batches.pop(0) if batches else None
        lock.release()
        if newBatch:
            duration = random.randint(0, COMPUTE_MAX) + 1
            newBatch.processingStartTime = int(time.time())
            time.sleep(duration)
            newBatch.processingEndTime = int(time.time())
            send(newBatch)
            print("computed: %d " % duration)


def send(batch: BatchInfo):
    record_key = batch.appId
    record_value = json.dumps(batch, cls=MyEncoder)
    producer.send("test_input", bytes(record_value, encoding="utf-8"), bytes(record_key, encoding="utf-8"))
    producer.flush()


if __name__ == '__main__':
    # 创建两个线程
    try:
        _thread.start_new_thread(submit_job, ())
        _thread.start_new_thread(compute_job, ())
        while True:
            print("pending: %d" % len(batches))
            time.sleep(5)
    except Exception as e:
        print(e)
