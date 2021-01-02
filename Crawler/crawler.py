import sys
from time import sleep, time
from loguru import logger
from .core.base_storage import BaseStorage

SLEEP_TIME_BETWEEN_THREAD_DEPLOY = 5


class Crawler:
    def __init__(
        self,
        base_url: str,
        depth: int,
        worker_class,
        storage: BaseStorage,
        workers_number=4,
        **worker_assets
    ):
        self.base_url = base_url
        self.depth = depth

        self.workers = []
        self.worker_class = worker_class
        self.workers_number = workers_number

        self.storage = storage
        self.worker_assets = worker_assets
        self.setup_logger()

    def setup_logger(self):
        logger.remove()
        logger.add(
            sys.stderr,
            colorize=True,
            enqueue=True,
            format="<blue>{time}</blue> | <green>{level}</green> | {message}",
            filter=lambda record: record["module"] == "crawler"
        )
        logger.add(
            sys.stderr,
            colorize=True,
            enqueue=True,
            format="<blue>{time}</blue> | <green>{level}</green> | <yellow>{thread}</yellow> | {message}",
            filter=lambda record: record["module"] == "base_worker"
        )
        logger.add(
            sys.stderr,
            colorize=True,
            enqueue=True,
            format="<blue>{time}</blue> | <green>{level}</green> | <blue>{module}</blue> | {message}",
            filter=lambda record: record["module"] not in ("crawler", "base_worker")
        )

    def create_workers(self):
        logger.info("Creating workers.")
        for worker_id in range(self.workers_number):
            self.worker_assets["worker_id"] = worker_id
            worker = self.worker_class(self.storage, **self.worker_assets)
            self.workers.append(worker)
        self.worker_assets.pop("worker_id", None)

    def run_workers(self):
        logger.info("Run workers.")
        for index, worker in enumerate(self.workers):
            worker.run()  # start thread
            logger.info(f"Worker number <{index+1}> is running.")
            if index + 1 != len(self.workers):
                sleep(SLEEP_TIME_BETWEEN_THREAD_DEPLOY)

    def stop_workers(self):
        logger.info("Stops workers.")
        for index, worker in enumerate(self.workers):
            worker.stop()  # stop thread
            logger.info(f"Worker number <{index}> has stopped.")

    def idle(self, timeout=None):
        """
        :param timeout: time to sleep in seconds
        """
        try:
            start_time = time()
            while True:
                sleep(1)
                if timeout and timeout > time() - start_time:
                    break
        except KeyboardInterrupt:
            self.stop_workers()
            self.storage.stop()
            sleep(1)
            logger.warning("-" * 10 + "Shutdown in 10 seconds!" + "-" * 10)
            sleep(10)
