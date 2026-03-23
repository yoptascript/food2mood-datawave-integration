"""
Фоновая задача для real-time синхронизации профиля пользователя с DataWave.

Вызывается из FastAPI BackgroundTasks при изменении профиля:

    from tasks.realtime import push_profile_to_datawave
    from fastapi import BackgroundTasks

    @router.put("/profile")
    async def update_profile(data: ProfileUpdate, background_tasks: BackgroundTasks):
        # ... обновляем профиль в БД ...
        background_tasks.add_task(
            push_profile_to_datawave,
            phone=user.phone,
            fio=user.name,
            properties={...},
        )
"""

import logging
from typing import Any

from datawave.client import DataWaveClient


logger = logging.getLogger("DataWaveClient")


async def push_profile_to_datawave(
    phone: str,
    fio: str,
    properties: dict[str, Any],
) -> None:
    """
    Отправляет профиль пользователя в DataWave в фоне.

    Не бросает исключения — все ошибки логируются внутри клиента.
    Основной API Food2Mood не пострадает даже если DataWave недоступен.

    Аргументы:
        phone — телефон пользователя (10 цифр без +7/8)
        fio — имя пользователя
        properties — дополнительные поля профиля
    """
    client = DataWaveClient()
    success = await client.submit_profile(phone, fio, properties)

    if success:
        logger.info("push_profile_to_datawave | OK | phone=%s", phone)
    else:
        logger.warning("push_profile_to_datawave | FAIL | phone=%s", phone)
