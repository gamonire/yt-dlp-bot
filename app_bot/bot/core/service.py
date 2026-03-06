import logging
import re
from itertools import product
from urllib.parse import urljoin, urlparse

from pyrogram.types import Message
from yt_shared.clients.github import YtdlpGithubClient
from yt_shared.constants import REMOVE_QUERY_PARAMS_HOSTS
from yt_shared.enums import TaskSource, TelegramChatType, YtdlpReleaseChannelType
from yt_shared.repositories.ytdlp import YtdlpRepository
from yt_shared.rabbit.publisher import RmqPublisher
from yt_shared.schemas.media import InbMediaPayload
from yt_shared.schemas.url import URL
from yt_shared.schemas.ytdlp import VersionContext
from yt_shared.ytdlp.version_checker import YtdlpVersionChecker

from bot.core.schemas import UserSchema
from bot.core.utils import bold, can_remove_url_params, code


class UrlService:
    def __init__(self) -> None:
        self._log = logging.getLogger(self.__class__.__name__)
        self._rmq_publisher = RmqPublisher()

    async def process_urls(self, urls: list[URL]) -> None:
        for url in urls:
            await self._send_to_worker(url)

    async def _send_to_worker(self, url: URL) -> bool:
        payload = InbMediaPayload(
            url=url.url,
            original_url=url.original_url,
            message_id=url.message_id,
            ack_message_id=url.ack_message_id,
            from_user_id=url.from_user_id,
            from_chat_id=url.from_chat_id,
            from_chat_type=url.from_chat_type,
            source=TaskSource.BOT,
            save_to_storage=url.save_to_storage,
            download_media_type=url.download_media_type,
            custom_filename=None,
            automatic_extension=False,
        )
        is_sent = await self._rmq_publisher.send_for_download(payload)
        if not is_sent:
            self._log.error('Failed to publish URL %s to message broker', url.url)
        return is_sent


class UrlParser:
    def __init__(self) -> None:
        self._log = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _preprocess_urls(urls: list[str]) -> dict[str, str]:
        preprocessed_urls: dict[str, str] = {}
        for url in urls:
            if can_remove_url_params(url=url, matching_hosts=REMOVE_QUERY_PARAMS_HOSTS):
                preprocessed_urls[url] = urljoin(url, urlparse(url).path)
            else:
                preprocessed_urls[url] = url
        return preprocessed_urls

    def parse_urls(
        self, urls: list[str], context: dict[str, Message | UserSchema]
    ) -> list[URL]:
        message: Message = context['message']
        user: UserSchema = context['user']
        ack_message: Message = context['ack_message']
        from_user_id = message.from_user.id if message.from_user else None
        return [
            URL(
                url=url,
                original_url=orig_url,
                from_chat_id=message.chat.id,
                from_chat_type=TelegramChatType(message.chat.type.value),
                from_user_id=from_user_id,
                message_id=message.id,
                ack_message_id=ack_message.id,
                save_to_storage=user.save_to_storage,
                download_media_type=user.download_media_type,
            )
            for orig_url, url in self._preprocess_urls(urls).items()
        ]

    def filter_urls(self, urls: list[str], regexes: list[str]) -> list[str]:
        """Return valid urls."""
        self._log.debug('Matching urls: %s against regexes %s', urls, regexes)
        valid_urls: list[str] = []
        for url, regex in product(urls, regexes):
            if re.match(regex, url):
                valid_urls.append(url)

        valid_urls = list(dict.fromkeys(valid_urls))
        self._log.debug('Matched urls: %s', valid_urls)
        return valid_urls


class YtdlpService:
    def __init__(
        self,
        repository: YtdlpRepository,
        release_channel: YtdlpReleaseChannelType,
    ) -> None:
        self._checker = YtdlpVersionChecker(
            client=YtdlpGithubClient(release_channel),
            repository=repository,
        )
        self._release_channel = release_channel

    async def get_version_context(self) -> VersionContext:
        return await self._checker.get_version_context()

    def format_check_text(self, ctx: VersionContext) -> str:
        status = '🟢 Up to date'
        if ctx.has_new_version:
            status = '🟠 Update available'

        return (
            f'{bold("yt-dlp status")} ({self._release_channel} channel)\n'
            f'Status: {bold(status)}\n'
            f'Current: {code(ctx.current.version)}\n'
            f'Latest: {code(ctx.latest.version)}'
        )

    def format_update_text(self, ctx: VersionContext) -> str:
        if not ctx.has_new_version:
            return (
                f'✅ {bold("yt-dlp is already up to date")}: '
                f'{code(ctx.current.version)}'
            )

        return (
            f'⬆️ New {self._release_channel} {code("yt-dlp")} version is available: '
            f'{bold(ctx.latest.version)}\n'
            f'Current version: {bold(ctx.current.version)}\n'
            f'Update command:\n'
            f'{code("docker compose build --no-cache yt_worker && docker compose up -d -t 0 yt_worker")}\n'
            f'After rebuild run {code("/yt-check")} to verify the new version.'
        )
