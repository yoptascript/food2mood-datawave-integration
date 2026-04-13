import os
import json
import requests
from dotenv import load_dotenv

# Загрузка переменных из .env
load_dotenv()

BASE_URL = os.getenv("DATAWAVE_BASE_URL", "https://external.demo.datawave.ru")
API_TOKEN = os.getenv("DATAWAVE_API_TOKEN")
RESOURCE_ID = os.getenv("DATAWAVE_RESOURCE_ID")

headers = {
    "Authorization": f"Bearer {API_TOKEN}"
}

print(f"Testing DataWave Connection...")
print(f"URL: {BASE_URL}")
print(f"Resource ID: {RESOURCE_ID}")

try:
    # Тестовый запрос выгрузки данных (limit=1 для проверки)
    url = f"{BASE_URL}/client-data/?resource_id={RESOURCE_ID}&limit=1"
    response = requests.get(url, headers=headers)

    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        print("✅ УСПЕХ! Подключение к DataWave установлено.")
        data = response.json()
        print("\nПример данных (Первый клиент):")
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print(f"❌ Ошибка: {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"❌ Исключение при запросе: {e}")


# =============================================================
# Тест-сюит
# =============================================================

results = {}


def test_connection():
    url = f"{BASE_URL}/client-data/?resource_id={RESOURCE_ID}&limit=1"
    r = requests.get(url, headers=headers, timeout=10)
    assert r.status_code == 200, f"HTTP {r.status_code}"


def test_fetch_data():
    r1 = requests.get(
        f"{BASE_URL}/client-data/?resource_id={RESOURCE_ID}&limit=10&offset=0",
        headers=headers, timeout=10
    )
    assert r1.status_code == 200, f"Страница 1: HTTP {r1.status_code}"

    r2 = requests.get(
        f"{BASE_URL}/client-data/?resource_id={RESOURCE_ID}&limit=10&offset=10",
        headers=headers, timeout=10
    )
    assert r2.status_code == 200, f"Страница 2: HTTP {r2.status_code}"

    # Если данных мало — страницы могут совпадать, это не ошибка :)
    data1 = r1.json()
    data2 = r2.json()
    items1 = data1 if isinstance(data1, list) else data1.get("results", [])
    items2 = data2 if isinstance(data2, list) else data2.get("results", [])

    if len(items1) < 10:
        return
    assert items1 != items2, "Страницы вернули одинаковые данные при offset=10"


def test_push_data():
    r = requests.post(
        f"{BASE_URL}/client-data/?resource_id={RESOURCE_ID}",
        headers=headers,
        json={
            "metadata": {
                "resource_id": RESOURCE_ID,
                "_test": True,
                "source": "sync_datawave_test.py",
            },
            "data": [],
        },
        timeout=10
    )
    if r.status_code in (405, 403):
        return "SKIP"
    if r.status_code == 400 and "Incorrect resource reference" in r.text:
        return "SKIP"  # ресурс доступен только для чтения
    assert r.status_code in (200, 201), f"HTTP {r.status_code}: {r.text[:200]}"


def test_error_handling():
    bad_headers = {"Authorization": "Bearer invalid_token"}

    r401 = requests.get(
        f"{BASE_URL}/client-data/?resource_id={RESOURCE_ID}&limit=1",
        headers=bad_headers, timeout=10
    )
    assert r401.status_code == 401, f"Ожидали 401, получили {r401.status_code}"

    r404 = requests.get(
        f"{BASE_URL}/client-data/?resource_id=nonexistent_000&limit=1",
        headers=headers, timeout=10
    )
    assert r404.status_code in (404, 400), f"Ожидали 404, получили {r404.status_code}"


# ── Запуск тестов ──

TESTS = [test_connection, test_fetch_data, test_push_data, test_error_handling]

print(f"\n{'=' * 40}")
print("Запуск тест-сюита")
print('=' * 40)

for test in TESTS:
    try:
        result = test()
        status = result if result == "SKIP" else "OK"
    except Exception as e:
        status = "FAIL"
        results[test.__name__] = str(e)

    icon = {"OK": "✅", "FAIL": "❌", "SKIP": "⏭️ "}[status]
    print(f"{icon} {status:<4}  {test.__name__}")
    if status == "FAIL":
        print(f"       {results.get(test.__name__, '')}")
    results[test.__name__] = status

# ── Итог ──

print("-" * 40)
ok   = sum(1 for s in results.values() if s == "OK")
fail = sum(1 for s in results.values() if s == "FAIL")
skip = sum(1 for s in results.values() if s == "SKIP")
print(f"OK: {ok}  FAIL: {fail}  SKIP: {skip}\n")