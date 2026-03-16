import asyncio
import logging
import os
import subprocess
import sys

from yt_dlp import version as ytdlp_version
from yt_shared.clients.github import YtdlpGithubClient
from yt_shared.db.session import get_db
from yt_shared.enums import YtdlpReleaseChannelType
from yt_shared.rabbit import get_rabbitmq
from yt_shared.rabbit.rabbit_config import INPUT_QUEUE
from yt_shared.repositories.ytdlp import YtdlpRepository
from yt_shared.utils.common import register_shutdown

from worker.core.callbacks import rmq_callbacks as cb
from worker.core.config import settings


class WorkerLauncher:
    _RUN_FOREVER_SLEEP_SECONDS = 86400

    def __init__(self) -> None:
        self._log = logging.getLogger(self.__class__.__name__)
        self._rabbit_mq = get_rabbitmq()
        self._auto_update_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._log.info('Starting download worker instance')
        await self._start()

    async def _start(self) -> None:
        await self._perform_setup()
        await self._run_forever()

    async def _run_forever(self) -> None:
        while True:
            await asyncio.sleep(self._RUN_FOREVER_SLEEP_SECONDS)

    async def _perform_setup(self) -> None:
        await self._create_intermediate_directories()
        await self._auto_update_yt_dlp_if_needed()
        await asyncio.gather(*(self._setup_rabbit(), self._set_yt_dlp_version()))
        self._register_shutdown()
        self._auto_update_task = asyncio.create_task(self._ytdlp_auto_update_loop())

    async def _auto_update_yt_dlp_if_needed(self) -> None:
        if not settings.YTDLP_AUTO_UPDATE_ENABLED:
            self._log.info('yt-dlp auto update is disabled')
            return

        latest = await self._safe_get_latest_yt_dlp_version()
        if not latest:
            return

        current = ytdlp_version.__version__
        if current == latest:
            self._log.info('yt-dlp is up to date: %s', current)
            return

        self._log.warning('Updating yt-dlp from %s to %s', current, latest)
        if not self._run_ytdlp_update_command():
            return

        self._log.warning(
            'yt-dlp update finished. Restarting process to apply new version'
        )
        os.execv(sys.executable, [sys.executable, *sys.argv])  # noqa: S606

    async def _safe_get_latest_yt_dlp_version(self) -> str | None:
        try:
            latest = await YtdlpGithubClient(
                release_channel=YtdlpReleaseChannelType.STABLE
            ).get_latest_version()
        except Exception:
            self._log.exception('Failed to fetch latest yt-dlp version')
            return None

        self._log.info('Latest yt-dlp version on GitHub: %s', latest.version)
        return latest.version

    def _run_ytdlp_update_command(self) -> bool:
        cmd = [
            sys.executable,
            '-m',
            'pip',
            'install',
            '--upgrade',
            '--pre',
            'yt-dlp',
        ]
        self._log.info('Running yt-dlp update command: %s', cmd)
        try:
            result = subprocess.run(  # noqa: S603
                cmd, check=False, capture_output=True, text=True
            )
        except Exception:
            self._log.exception('Failed to start yt-dlp update command')
            return False

        if result.returncode != 0:
            self._log.error(
                'yt-dlp update command failed with code %s. stdout: %s, stderr: %s',
                result.returncode,
                result.stdout,
                result.stderr,
            )
            return False

        self._log.info('yt-dlp update command output: %s', result.stdout.strip())
        return True

    async def _ytdlp_auto_update_loop(self) -> None:
        while True:
            await asyncio.sleep(settings.YTDLP_AUTO_UPDATE_INTERVAL_SECONDS)
            await self._auto_update_yt_dlp_if_needed()

    async def _setup_rabbit(self) -> None:
        self._log.info('Setting up RabbitMQ connection')
        await self._rabbit_mq.register()
        await self._rabbit_mq.channel.set_qos(
            prefetch_count=settings.MAX_SIMULTANEOUS_DOWNLOADS
        )
        await self._rabbit_mq.queues[INPUT_QUEUE].consume(cb.on_input_message)

    async def _set_yt_dlp_version(self) -> None:
        curr_version = ytdlp_version.__version__
        self._log.info(
            'Saving current yt-dlp version (%s) to the database', curr_version
        )
        async for db in get_db():
            await YtdlpRepository(db).create_or_update_version(curr_version)

    async def _create_intermediate_directories(self) -> None:
        """Create temporary intermediate directories on start if they do not exist."""
        tmp_download_path = settings.TMP_DOWNLOAD_ROOT_PATH / settings.TMP_DOWNLOAD_DIR
        tmp_downloaded_path = (
            settings.TMP_DOWNLOAD_ROOT_PATH / settings.TMP_DOWNLOADED_DIR
        )
        self._log.info(
            'Creating intermediate directories %s, %s if not exist',
            tmp_download_path,
            tmp_downloaded_path,
        )
        tmp_download_path.mkdir(parents=True, exist_ok=True)
        tmp_downloaded_path.mkdir(parents=True, exist_ok=True)

    def _register_shutdown(self) -> None:
        register_shutdown(self.stop)

    def stop(self, *args) -> None:  # noqa: ARG002
        self._log.info('Shutting down %s', self.__class__.__name__)
        if self._auto_update_task:
            self._auto_update_task.cancel()

        loop = asyncio.get_running_loop()
        loop.create_task(self._rabbit_mq.close())  # noqa: RUF006
