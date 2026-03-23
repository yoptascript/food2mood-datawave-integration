"""
Крон-задача для батч-синхронизации пользователей с DataWave.

Запускается раз в несколько часов (например через celery beat или apscheduler).

Пример запуска вручную:
    python -m tasks.cron
"""

import asyncio
import logging
from typing import Callable, Awaitable

from datawave.client import DataWaveClient, mock_get_users_from_db_func, mock_mark_users_as_synced


logger = logging.getLogger("DataWaveClient")


async def run_datawave_sync(
    get_users_func: Callable[[], Awaitable[list[dict]]],
    mark_synced_func: Callable[[list[str]], Awaitable[None]],
) -> None:
    """
    Запускает полную синхронизацию всех несинхронизированных пользователей.

    Аргументы:
        get_users_func — async функция без аргументов, возвращает список юзеров
                         у которых is_datawave_synced=False
        mark_synced_func — async функция, принимает список телефонов,
                           помечает их как is_datawave_synced=True
    """
    logger.info("run_datawave_sync | start")

    client = DataWaveClient()
    await client.sync_all_pending_users(get_users_func, mark_synced_func)

    logger.info("run_datawave_sync | done")


if __name__ == "__main__":
    # Запуск вручную для отладки — используем заглушки из client.py
    asyncio.run(run_datawave_sync(mock_get_users_from_db_func, mock_mark_users_as_synced))
