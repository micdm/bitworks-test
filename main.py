#!/usr/bin/env python3

from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from typing import Tuple

from lib.background import BackgroundMaster, JobValidator
from lib.common import logger
from lib.data import DataDownloader, DataReader, DataWriter
from lib.http import CustomServer, RequestHandler


LISTEN_ADDRESS = ('', 8888)


def parse_args() -> Tuple[int, int]:
    parser = ArgumentParser()
    parser.add_argument('--workers', required=True, type=int)
    parser.add_argument('--max-concurrency', type=int, default=50)
    args = parser.parse_args()
    return args.workers, args.max_concurrency


def run():
    worker_count, max_concurrency = parse_args()
    with TemporaryDirectory() as work_dir:
        logger.info('Temporary directory is %s', work_dir)
        downloader = DataDownloader(work_dir, worker_count)
        master = BackgroundMaster(JobValidator(max_concurrency), downloader, DataReader(work_dir), DataWriter(work_dir))
        server = CustomServer(LISTEN_ADDRESS, RequestHandler, on_new_job=master.add_job,
                              on_get_job_status=master.get_job_status)
        master.start(worker_count)
        try:
            server.serve_forever()
        finally:
            master.stop()
            downloader.stop()


if __name__ == '__main__':
    run()
