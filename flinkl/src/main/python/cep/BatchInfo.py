import json
import time
from json import JSONEncoder
from typing import Any


class BatchInfo:
    def __init__(self, appId: str, batchTime: int = int(time.time())):
        self.appId = appId
        self.batchTime = batchTime
        self.submissionTime = -1
        self.processingStartTime = -1
        self.processingEndTime = -1

    def __str__(self) -> str:
        return "BatchInfo(%s,%d)" % (self.appId, self.batchTime)


class MyEncoder(JSONEncoder):

    def default(self, o: Any) -> Any:
        return o.__dict__


if __name__ == "__main__":
    b1 = BatchInfo("app1")
    print(b1)
    print(MyEncoder().encode(b1))
    j = json.dumps(b1, cls=MyEncoder)
    print(j)

# def submit_batch():
#     # batchs.append()

# def complete_batch():
