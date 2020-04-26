from pyrogram.api.functions.messages import Search
from pyrogram.api.types import MessageEntityTextUrl, MessageEntityUrl, MessageEntityMention
from pyrogram.errors import FloodWait, UsernameInvalid
from time import sleep
from string import ascii_letters, digits

from Crawler.core.base_worker import BaseWorker


class Worker(BaseWorker):
    def __init__(self, storage, **kwargs):
        self.userbot = kwargs['userbot']
        self.messages_filter = kwargs['messages_filter']
        super().__init__(storage)

    def flood_handler(self, FW):
        print("Flood sleep:", FW.x)
        sleep(FW.x*5)

    def is_telegram_link(self, clean_url):
        if 't.me/' not in clean_url:
            return False
        return True

    def reper_broken_link(self, link):
        link_letters = '/._-'
        for letter_index, letter in enumerate(link):
            if letter not in ascii_letters+digits+link_letters:
                return link[:letter_index]
        return link

    def post_link_to_chat_link(self, link):
        last_part = link.split('/')[-1]
        is_digest = True
        for letter in last_part:
            if letter not in digits:
                is_digest=False
                break
        if is_digest:
            return link[:link.rfind('/')]
        return link

    def clean_link(self, link):
        """
        http://t.me/<channel name> => <channel name>
        https://t.me/joinchat/<token> => https://t.me/joinchat/<token>
        :param link: telegram link
        :return: public link as channel name and private link as the same
        """
        if 't.me/' not in link:
            return link
        if "joinchat" not in link:
            before_username = 't.me/'
            link = link[link.find(before_username)+len(before_username):]
            link = self.post_link_to_chat_link(link)
            return link
        else:
            link = link[link.find('t.me/'):]
            link = self.reper_broken_link(link)  # if not broken return the same link
            return link

    def get_chat(self, clean_link):
        try:
            if "joinchat" in clean_link:
                self.userbot.join_chat(clean_link)
            chat = self.userbot.get_chat(clean_link)
        except FloodWait as FW:
            self.flood_handler(FW)
            chat = self.get_chat(clean_link)
        return chat

    def get_link_messages(self, peer):
        messages = self.userbot.send(
                Search(peer=peer,
                       filter = self.messages_filter,
                       q="",
                       offset_id=0,
                       add_offset=0,
                       limit=99999999,
                       max_id=0,
                       min_id=0,
                       min_date=0,
                       max_date=0,
                       hash=0)
                )
        return messages

    def url_from_entity(self, entity, message):
        if hasattr(entity, 'url'):
            return entity.url
        else:
            return message.message[entity.offset:entity.offset+entity.length]

    def extract_links_from_message(self, message):
        entities = message.entities
        telegram_links = set()
        for entity in entities:
            if type(entity) not in (MessageEntityTextUrl,
                                    MessageEntityUrl,
                                    MessageEntityMention):
                continue
            url = self.url_from_entity(entity, message)
            if self.is_telegram_link(url):
                telegram_links.add(self.clean_link(url))
        return telegram_links

    def return_links_from_messages(self, messages):
        telegram_links = set()
        for message in messages.messages:
            telegram_links |= self.extract_links_from_message(message)
        return telegram_links

    def cleanup(self, chat, link):
        if "joinchat" in link:
            self.userbot.leave_chat(chat.id, delete=True)

    def find_sublinks(self, link):
        sleep(5)
        clean_link = self.clean_link(link)
        try:
            chat = self.get_chat(clean_link)
        except UsernameInvalid:
            return []
        peer = self.userbot.resolve_peer(chat.id)
        link_messages = self.get_link_messages(peer)
        telegram_links = self.return_links_from_messages(link_messages)
        self.cleanup(chat, clean_link)
        return telegram_links
