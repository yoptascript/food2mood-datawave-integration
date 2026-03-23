import os
import logging
import httpx
import re
import asyncio

logging.basicConfig(
    filename="DataWaveClient.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

logger = logging.getLogger("DataWaveClient")


# --- MOCK FUNCTION (Заглушка БД) ---
async def mock_get_users_from_db_func():
    """
    Имитация обращения к БД.
    Возвращает список словарей, который ожидает sync_all_pending_users.
    """
    await asyncio.sleep(0.5)

    users = []
    for i in range(450):
        users.append({
            "Phone": f"900{i:07d}",  # Генерируем 10 цифр
            "User Name": f"Тестовый Юзер {i}",
            "Diets": ["веган"] if i % 2 == 0 else [],
            "Hate": ["орехи"],
            "Cart Items": ["Блюдо 1", "Блюдо 2"]
        })

    logger.info(f"Mock DB | Извлечено {len(users)} пользователей")
    return users

# --- MOCK FUNCTION (Заглушка обновления статуса синхронизации пользователя в БД) ---
async def mock_mark_users_as_synced(phones_list):
    """
    Заглушка: имитируем UPDATE users SET is_synced=True WHERE phone IN (...)
    """
    await asyncio.sleep(0.1)
    print(f"[DB] Статус синхронизации обновлен для {len(phones_list)} номеров")
    return True

class DataWaveClient:
    def __init__(self):
        self.base_url = os.getenv("DATAWAVE_BASE_URL")
        self.api_token = os.getenv("DATAWAVE_API_TOKEN")
        self.resource_id = os.getenv("DATAWAVE_RESOURCE_ID")
        self.headers = {"Authorization": f"Bearer {self.api_token}"}

    @staticmethod
    def _validate_payload(phone: str, fio: str, properties: dict) -> list[str]:
        """
        Валидация данных перед отправкой в DataWave.

        Returns:
            Список строк с описанием ошибок. Пустой список — данные корректны.
        """
        errors = []

        if not phone:
            errors.append("phone: обязательное поле, значение отсутствует")
        elif not isinstance(phone, str):
            errors.append(f"phone: ожидается str, получен {type(phone).__name__}")
        elif not re.fullmatch(r"\d{10}", phone):
            errors.append(
                f"phone: ожидается ровно 10 цифр без +7/8, получено «{phone}»"
            )

        if not fio:
            errors.append("fio: обязательное поле, значение отсутствует")
        elif not isinstance(fio, str):
            errors.append(f"fio: ожидается str, получен {type(fio).__name__}")
        elif not fio.strip():
            errors.append("fio: передана пустая строка")

        if "eats_preferences" in properties:
            ep = properties["eats_preferences"]
            if not isinstance(ep, list):
                errors.append(
                    f"eats_preferences: ожидается list, получен {type(ep).__name__}"
                )
            elif not all(isinstance(i, str) for i in ep):
                errors.append("eats_preferences: все элементы списка должны быть str")

        if "hate_components" in properties:
            hc = properties["hate_components"]
            if not isinstance(hc, list):
                errors.append(
                    f"hate_components: ожидается list, получен {type(hc).__name__}"
                )
            elif not all(isinstance(i, str) for i in hc):
                errors.append("hate_components: все элементы списка должны быть str")

        if "purchase_history" in properties:
            ph = properties["purchase_history"]
            if not isinstance(ph, list):
                errors.append(
                    f"purchase_history: ожидается list, получен {type(ph).__name__}"
                )
            elif not all(isinstance(i, str) for i in ph):
                errors.append("purchase_history: все элементы списка должны быть str")

        return errors


    async def fetch_updates_by_phone(self, phone: str) -> dict:
        """Точечный запрос одного пользователя (On-Demand)"""
        url = f"{self.base_url}/client-data/?resource_id={self.resource_id}&phone={phone}"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=self.headers)
                if response.status_code == 200:
                    data = response.json()
                    return data["items"][0] if data.get("items") else {}
                return {}
            except Exception as e:
                logger.error("DataWave | fetch error | phone=%s | error=%s", phone, e)
                return {}

    async def submit_profile(self, phone: str, fio: str, properties: dict) -> bool:
        """
        Реал-тайм выгрузка данных пользователя в DataWave (Push).

        Вызывается как фоновая задача при любом изменении профиля пользователя:
        обновление предпочтений, покупка, изменение имени и т.д.
        Не блокирует основной API Food2Mood.

        Карта полей properties (все ключи опциональны):
            - eats_preferences (list) — предпочтения (напр. ["веган"])
            - hate_components (list) — исключаемые ингредиенты (напр. ["арахис"])
            - purchase_history (list) — история покупок (список блюд)

        Args:
            phone: Номер телефона (10 цифр, без +7/8) — ключ матчинга.
            fio:        Имя и фамилия клиента.
            properties: Словарь свойств пользователя согласно карте полей выше.

        Returns:
            True  — DataWave принял данные (200 / 201).
            False — ошибка валидации, сеть, тайм-аут или неожиданный статус.
        """
        # --- валидация до отправки ---
        errors = self._validate_payload(phone, fio, properties)
        if errors:
            for err in errors:
                logger.error("DataWave | validation error | phone=%s | %s", phone, err)
            return False

        url = f"{self.base_url}/client-data/"
        payload = {
            "metadata": {"resource_id": self.resource_id},
            "data": [{"phone": phone, "fio": fio, "properties": properties}],
        }
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, headers=self.headers, json=payload)

                if response.status_code in (200, 201):
                    logger.info(
                        "DataWave | push OK | phone=%s | status=%s",
                        phone, response.status_code,
                    )
                    return True

                logger.warning(
                    "DataWave | push unexpected status | phone=%s | status=%s | body=%s",
                    phone, response.status_code, response.text[:500],
                )
                return False

            except Exception as e:
                logger.error("DataWave | push error | phone=%s | error=%s", phone, e)
                return False

    async def submit_bulk_profiles(self, users_batch: list[dict]) -> bool:
        """
        Отправка группы пользователей (до 200 человек) одним запросом.
        users_batch — это список словарей, где каждый словарь уже в формате DataWave.
        """
        url = f"{self.base_url}/client-data/"

        payload = {
            "metadata": {"resource_id": self.resource_id},
            "data": users_batch  # Здесь уже список из многих пользователей
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.post(url, headers=self.headers, json=payload)
                if response.status_code in (200, 201):
                    logger.info("DataWave | bulk push OK | count=%s", len(users_batch))
                    return True

                logger.error("DataWave | bulk push fail | status=%s | body=%s",
                             response.status_code, response.text[:200])
                return False
            except Exception as e:
                logger.error("DataWave | bulk push error | error=%s", e)
                return False


## kotofich changes
# - added pending func and logger in it
# - added batch slicing
# - added mock function getting_users_from_bd before class initialisation.
# - added mock function which is updating user status after we actualy send them into datawave

    async def sync_all_pending_users(self, get_users_from_db_func, mark_as_synced_func):
        """
        Основная логика крона:
        1. Получает всех 'грязных' пользователей.
        2. Валидирует и готовит их.
        3. Нарезает по 200 и отправляет.
        """
        # moked func get_users_from_db_func() - у нас нет доступа к бд, поэтому заглушка.
        # функция из БД в будущем должна возвращать список объектов/словарей
        raw_users = await get_users_from_db_func()

        if not raw_users:
            logger.info("DataWave | Sync | No users to sync")
            return

        prepared_data = []
        for user in raw_users:
            user_data = {
                "phone": user['Phone'],
                "fio": user['User Name'],
                "properties": {
                    "eats_preferences": user.get('Diets', []),
                    "hate_components": user.get('Hate', []),
                    "purchase_history": user.get('Cart Items', [])
                }
            }

            # Валидируем перед добавлением в пачку
            errors = self._validate_payload(user_data["phone"], user_data["fio"], user_data["properties"])
            if not errors:
                prepared_data.append(user_data)
            else:
                logger.warning("DataWave | Sync | Skip user %s due to validation errors", user['Phone'])

        # Нарезка на батчи по 200
        batch_size = 200
        for i in range(0, len(prepared_data), batch_size):
            batch = prepared_data[i: i + batch_size]

            success = await self.submit_bulk_profiles(batch)
            if success:
                # Тут надо вызвать функцию БД, которая пометит этих юзеров как 'synchronized'
                synced_phones = [u["phone"] for u in batch]

                await mark_as_synced_func(synced_phones)

                logger.info("DataWave | Sync | Batch %s marked as synced in BD", i // batch_size + 1)
                pass