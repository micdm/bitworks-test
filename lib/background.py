from queue import Queue
from threading import Thread
from typing import Optional
from urllib.parse import urlparse
from uuid import uuid4

from .common import Job, CustomException, logger, JobStatus, merge_int_iterables
from .data import DataDownloader, DataReader, DataWriter, DataSorter


class ValidatedJob:

    @classmethod
    def create_from(cls, job: Job):
        return cls(job.id, int(job.concurrency), job.url)

    def __init__(self, job_id: str, concurrency: int, url: str):
        self.id = job_id
        self.concurrency = concurrency
        self.url = url

    def __str__(self):
        return '%s(id=%s, concurrency=%s, url=%s)' % (self.__class__.__name__, self.id, self.concurrency, self.url)


class JobValidator:

    def __init__(self, max_concurrency: int):
        self._max_concurrency = max_concurrency

    def validate(self, job: Job) -> ValidatedJob:
        self._validate_concurrency(job.concurrency)
        self._validate_url(job.url)
        return ValidatedJob.create_from(job)

    def _validate_concurrency(self, concurrency: str):
        try:
            concurrency = int(concurrency)
        except ValueError:
            raise CustomException('concurrency must be integer')
        if concurrency <= 0:
            raise CustomException('concurrency must be positive')
        if concurrency > self._max_concurrency:
            raise CustomException('concurrency must be less than %s' % self._max_concurrency)

    def _validate_url(self, url: str):
        try:
            result = urlparse(url)
        except:
            raise CustomException('invalid URL')
        if result.scheme != 'http':
            raise CustomException('URL must be a HTTP resource')


class BackgroundMaster:

    def __init__(self, validator: JobValidator, downloader: DataDownloader, reader: DataReader, writer: DataWriter):
        self._queue = Queue()
        self._jobs = {}
        self._workers = []
        self._validator = validator
        self._downloader = downloader
        self._reader = reader
        self._writer = writer

    def start(self, worker_count: int):
        logger.info('Starting background master with %s workers', worker_count)
        for _ in range(worker_count):
            worker = BackgroundWorker(self._validator, self._downloader, self._reader, self._writer, self._queue,
                                      self._jobs)
            worker.start()
            self._workers.append(worker)

    def stop(self):
        logger.info('Stopping background master')
        for _ in self._workers:
            self._queue.put(None)

    def add_job(self, concurrency: str, url: str) -> str:
        job_id = str(uuid4())
        job = Job(job_id, concurrency, url)
        logger.debug('Adding job %s', job)
        self._queue.put(job)
        self._jobs[job_id] = JobStatus.queued()
        return job_id

    def get_job_status(self, job_id: str) -> Optional[JobStatus]:
        return self._jobs.get(job_id)


class BackgroundWorker:

    def __init__(self, validator: JobValidator, downloader: DataDownloader, reader: DataReader, writer: DataWriter,
                 queue: Queue, jobs: dict):
        self._validator = validator
        self._downloader = downloader
        self._reader = reader
        self._writer = writer
        self._queue = queue
        self._jobs = jobs

    def start(self):
        logger.info('Starting background worker %s', self)
        Thread(target=self._run).start()

    def _run(self):
        while True:
            job = self._queue.get()
            if job is None:
                logger.debug('Closing background worker %s', self)
                break
            logger.debug('Got job %s, processing on %s', job, self)
            try:
                self._jobs[job.id] = JobStatus.progress()
                validated_job = self._validator.validate(job)
                path = self._process_job(validated_job)
                self._jobs[job.id] = JobStatus.ready(path)
            except Exception as e:
                logger.exception('Cannot process job %s: %s', job, e)
                self._jobs[job.id] = JobStatus.error(str(e) if isinstance(e, CustomException) else 'unexpected error')

    def _process_job(self, job: ValidatedJob) -> str:
        name = '%s.raw' % job.id
        self._downloader.download_url(job.url, name)
        with DataSorter(job.concurrency) as sorter:
            sorted_numbers = merge_int_iterables(sorter.sort(numbers) for numbers in self._reader.read(name))
            path = self._writer.write('%s.json' % job.id, sorted_numbers)
        return path
