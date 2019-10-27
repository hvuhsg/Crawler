from Crawler.StorageTypes.mongo_storage import Storage
from Crawler.crawler import Crawler
from worker import Worker


def main():
    crawler = Crawler(base_url='en.wikipedia.org/wiki/Main_Page', db_name='crawlerDB', depth=5, storage_class=Storage,
                      worker_class=Worker, workers_number=5, username='crawler_username', password="crawler_password")
    crawler.create_workers()
    crawler.run_workers()
main()
