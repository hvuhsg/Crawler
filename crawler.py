from time import sleep, time


class Crawler:
    def __init__(
        self,
        base_url,
        depth,
        worker_class,
        storage,
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
        for worker in self.workers:
            worker.start()  # start thread
            sleep(5)

    def stop_workers(self):
        for worker in self.workers:
            worker.stop()  # stop thread

    def idle(self, timeout=None):
        """

        :param timeout: time to sleep in seconds
        :return: None
        """

        try:
            start_time = time()
            while True:
                sleep(1)
                if timeout and timeout > time() - start_time:
                    break
        except KeyboardInterrupt:
            pass
