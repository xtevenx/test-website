import enum
import queue
import threading
import typing


@enum.unique
class Status(enum.Enum):
    IN_QUEUE = enum.auto()
    IN_PROGRESS = enum.auto()
    COMPLETE = enum.auto()


class JobWrapper:
    def __init__(self, job: typing.Callable) -> None:
        self.job: typing.Callable = job
        self.output: typing.Any = None
        self.status: Status = Status.IN_QUEUE

    def do_job(self) -> None:
        self.status = Status.IN_PROGRESS
        self.output = self.job()
        self.status = Status.COMPLETE


class SimpleQueue:
    def __init__(self) -> None:
        self._queue: queue.Queue[JobWrapper] = queue.Queue()

        self._worker_thread: threading.Thread = threading.Thread(target=self._worker)
        self._worker_thread.start()

    def add_job(self, job: typing.Callable) -> JobWrapper:
        self._queue.put(obj := JobWrapper(job))
        return obj

    def _worker(self) -> None:
        while next_job := self._queue.get(block=True, timeout=None):
            next_job.do_job()
