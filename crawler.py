from time import sleep, time
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

    def create_workers(self):
        for _ in range(self.workers_number):
            worker = self.worker_class(self.storage, **self.worker_assets)
            self.workers.append(worker)

    def run_workers(self):
        for index, worker in enumerate(self.workers):
            worker.run()  # start thread
            if index+1 != len(self.workers):
                sleep(SLEEP_TIME_BETWEEN_THREAD_DEPLOY)

    def stop_workers(self):
        for worker in self.workers:
            worker.stop()  # stop thread

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
            print('-'*10, "Shutdown in 10 seconds!", '-'*10)
            sleep(10)


