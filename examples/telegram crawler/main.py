from Crawler.storage_types.mongo_storage import Storage
from Crawler.crawler import Crawler
from worker import Worker

from pyrogram import Client
from pyrogram.api.types import InputMessagesFilterUrl


def main():
    base_url = "https://t.me/BonOgood"
    depth = 2
    userbot = Client("userbot")
    messages_filter = InputMessagesFilterUrl()

    worker_arguments = {"userbot": userbot, "messages_filter": messages_filter}

    userbot.start()

    mongo_storage = Storage(
        base_url=base_url,
        db_name="crawlerDB",
        username="crawler_username",
        password="crawler_password",
        max_depth=depth,
    )

    crawler = Crawler(
        base_url=base_url,
        depth=depth,
        storage=mongo_storage,
        worker_class=Worker,
        workers_number=1,
        **worker_arguments
    )
    crawler.create_workers()
    crawler.run_workers()


main()
