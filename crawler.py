from time import sleep


class Crawler:
    def __init__(
        self,
        base_url,
        depth,
        worker_class,
        storage_class,
        db_name,
        username,
        password,
        workers_number=4,
    ):
        self.base_url = base_url
        self.depth = depth

        self.workers = []
        self.worker_class = worker_class
        self.workers_number = workers_number

        self.storage = storage_class(db_name, base_url, self.depth, username, password)

    def create_workers(self):
        for _ in range(self.workers_number):
            worker = self.worker_class(self.storage)
            self.workers.append(worker)

    def run_workers(self):
        for worker in self.workers:
            worker.run()  # start thread
            sleep(5)

    def stop_workers(self):
        for worker in self.workers:
            worker.stop()  # stop thread
