import os
from math import ceil
from multiprocessing.pool import Pool
from queue import Queue
from shutil import copyfileobj
from threading import Thread
from typing import Optional, Iterable, List, IO
from urllib.error import HTTPError
from urllib.request import urlopen

from .common import logger, CustomException, merge_int_iterables, quick_sort


BUFFER_SIZE = 10240
DATA_SEPARATOR = ', '


class DataDownloader:

    def __init__(self, work_dir: str, pool_size: int):
        self._work_dir = work_dir
        self._pool = Pool(pool_size)

    def stop(self):
        logger.info('Stopping data downloader')
        self._pool.terminate()

    def download_url(self, url: str, output_name: str):
        self._pool.apply(self._download_url, (self._work_dir, url, output_name))

    @staticmethod
    def _download_url(work_dir: str, url: str, output_name: str):
        path = os.path.join(work_dir, output_name)
        logger.debug('Downloading URL %s into %s', url, path)
        try:
            with urlopen(url) as response:
                with open(path, 'wb') as output:
                    copyfileobj(response, output)
        except HTTPError as e:
            logger.debug('Cannot download %s: %s', url, e)
            raise CustomException('remote error')
        size = os.path.getsize(path)
        if not size:
            raise CustomException('data seems empty')
        logger.debug('Download of %s complete (%s bytes total)', url, size)


class DataReader:

    def __init__(self, work_dir: str):
        self._work_dir = work_dir

    def read(self, name: str) -> Iterable[List[int]]:
        path = os.path.join(self._work_dir, name)
        logger.info('Reading numbers from file %s', path)
        with open(path) as file:
            yield from self._read_from_file(file)

    def _read_from_file(self, file: IO, buffer_size=BUFFER_SIZE) -> Iterable[List[int]]:
        tail = None
        while True:
            chunk = file.read(buffer_size)
            if tail:
                chunk = tail + chunk
            if not chunk:
                break
            if len(chunk) < buffer_size:
                head, tail = chunk, None
            else:
                parts = chunk.rsplit(DATA_SEPARATOR, 1)
                if len(parts) == 2:
                    head, tail = parts
                else:
                    head, tail = parts[0], None
            try:
                yield list(map(int, head.split(DATA_SEPARATOR)))
            except ValueError as e:
                logger.warning('Cannot parse numbers: %s', e)
                raise CustomException('incorrect data')


class DataSorter:

    def __init__(self, concurrency: int):
        self._workers: List[DataSorterWorker] = []
        self._concurrency = concurrency

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()

    def _start(self):
        for _ in range(self._concurrency):
            worker = DataSorterWorker()
            worker.start()
            self._workers.append(worker)

    def _stop(self):
        for worker in self._workers:
            worker.put(None)

    def sort(self, numbers: List[int]) -> Iterable[int]:
        logger.debug('Sorting numbers of length %s', len(numbers))
        count = int(ceil(len(numbers) / self._concurrency))
        workers = []
        for worker, offset in zip(self._workers, range(0, len(numbers), count)):
            worker.put(numbers[offset:offset + count])
            workers.append(worker)
        return merge_int_iterables(worker.get() for worker in workers)


class DataSorterWorker:

    def __init__(self):
        self._input_queue = Queue()
        self._output_queue = Queue()

    def put(self, numbers: Optional[List[int]]):
        self._input_queue.put(numbers)

    def get(self) -> List[int]:
        return self._output_queue.get()

    def start(self):
        logger.debug('Starting data sorter worker %s', self)
        Thread(target=self._run).start()

    def _run(self):
        while True:
            numbers = self._input_queue.get()
            if numbers is None:
                logger.debug('Stopping data sorter worker %s', self)
                break
            try:
                result = quick_sort(numbers)
            except Exception as e:
                logger.warning('Cannot sort numbers on %s: %s', self, e)
                result = None
            self._output_queue.put(result)


class DataWriter:

    def __init__(self, work_dir: str):
        self._work_dir = work_dir

    def write(self, name: str, numbers: Iterable[int]) -> str:
        path = os.path.join(self._work_dir, name)
        logger.info('Writing sorted numbers into %s', path)
        with open(path, 'w') as file:
            file.write('{"state": "ready", "data": [')
            iterator = iter(numbers)
            file.write(str(next(iterator)))
            for number in iterator:
                file.write(', %s' % number)
            file.write(']}')
        return path
