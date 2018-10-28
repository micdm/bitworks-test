import logging
from random import randint
from typing import Iterable, List


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s]: %(message)s')
logger = logging.getLogger(__name__)


class CustomException(Exception):
    pass


class Job:

    def __init__(self, job_id: str, concurrency: str, url: str):
        self.id = job_id
        self.concurrency = concurrency
        self.url = url

    def __str__(self):
        return '%s(id=%s, concurrency=%s, url=%s)' % (self.__class__.__name__, self.id, self.concurrency, self.url)


class JobStatus:

    STATE_QUEUED = 'queued'
    STATE_PROGRESS = 'progress'
    STATE_READY = 'ready'
    STATE_ERROR = 'error'

    @classmethod
    def queued(cls):
        return JobStatus(cls.STATE_QUEUED)

    @classmethod
    def progress(cls):
        return JobStatus(cls.STATE_PROGRESS)

    @classmethod
    def ready(cls, path: str):
        return JobStatus(cls.STATE_READY, path)

    @classmethod
    def error(cls, message: str):
        return JobStatus(cls.STATE_ERROR, message)

    def __init__(self, state: str, data: str=None):
        self.state = state
        self.data = data

    def has_file_path(self):
        return self.state == self.STATE_READY


def merge_int_iterables(parts: Iterable[Iterable[int]]) -> Iterable[int]:
    def pair(numbers):
        iterator = iter(numbers)
        return [next(iterator), iterator]
    pairs = list(map(pair, parts))
    while pairs:
        min_index = None
        min_value = float('Inf')
        for i, (value, _) in enumerate(pairs):
            if value < min_value:
                min_index = i
                min_value = value
        try:
            pairs[min_index][0] = next(pairs[min_index][1])
        except StopIteration:
            del pairs[min_index]
        yield min_value


def quick_sort(numbers: List[int]) -> List[int]:
    if len(numbers) <= 1:
        return numbers
    pivot = numbers[randint(0, len(numbers) - 1)]
    head = quick_sort([item for item in numbers if item < pivot])
    tail = quick_sort([item for item in numbers if item > pivot])
    return head + [item for item in numbers if item == pivot] + tail
