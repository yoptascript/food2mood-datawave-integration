import os
import logging
import httpx
import re


logging.basicConfig(
    filename="DataWaveClient.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

logger = logging.getLogger("DataWaveClient")


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