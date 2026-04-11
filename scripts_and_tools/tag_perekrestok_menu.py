from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS: tuple[str, ...] = (
    "name",
    "category",
    "calories",
    "protein",
    "fat",
    "carbs",
    "weight_g",
)

MANDATORY_COLUMNS: tuple[str, ...] = (
    "name",
    "category",
    "calories",
    "protein",
    "fat",
    "carbs",
)

COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "name": (
        "name",
        "Краткий текст материала",
        "Название",
        "Наименован",
        "Краткое описание",
    ),
    "category": ("category", "УИ3 наименование", "УИ4 наименование", "Класс"),
    "calories": ("calories", "Калории / nutrition_facts.calories"),
    "protein": ("protein", "Белки / nutrition_facts.proteins"),
    "fat": ("fat", "Жиры / nutrition_facts.fats"),
    "carbs": ("carbs", "Углеводы / nutrition_facts.carbohydrates"),
    "weight_g": (
        "weight_g",
        "Вес порции",
        "Вес, г",
        "Вес (г)",
        "Масса порции",
        "weight",
    ),
}

CUISINE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "домашнее": (
        "по-домашнему",
        "по-деревенски",
        "по-французски",
        "классический",
        "традиционный",
        "котлета",
        "гуляш",
        "плов",
        "пюре",
        "каша",
        "борщ",
        "щи",
        "рассольник",
        "солянка",
        "оливье",
        "винегрет",
        "мимоза",
        "шницель",
        "зразы",
    ),
    "авторское": (
        "авторский",
        "премиум",
        "селект",
        "шеф",
        "концепт",
        "микс",
        "signature",
        "су-вид",
    ),
    "азиатское": (
        "вок",
        "удон",
        "лапша по-китайски",
        "димсамы",
        "гедза",
        "баоцзы",
        "суши",
        "роллы",
        "онигири",
        "рамен",
        "поке",
        "терияки",
        "унаги",
        "мисо",
        "эдамамэ",
        "том ям",
        "фо бо",
        "кимчи",
    ),
    "итальянское": (
        "спагетти",
        "паста",
        "пенне",
        "феттучини",
        "тортильони",
        "орзо",
        "ризотто",
        "пицца",
        "лазанья",
        "брускетта",
        "капрезе",
        "песто",
        "карбонара",
        "арабьята",
        "болоньезе",
        "альфредо",
    ),
}

_NON_NUMERIC_RE = re.compile(r"[^0-9,.\-]")
_WEIGHT_IN_NAME_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:г|гр|грамм?)\b", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Разметка меню Перекрёстка тегами по КБЖУ, сытности и кухне."
    )
    parser.add_argument("--input", required=True, help="Путь к входному Excel-файлу")
    parser.add_argument("--output", required=True, help="Путь к выходному Excel-файлу")
    return parser.parse_args()


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def to_float(value: object) -> float:
    if pd.isna(value):
        return 0.0
    raw = str(value).strip()
    if not raw:
        return 0.0
    cleaned = _NON_NUMERIC_RE.sub("", raw).replace(",", ".")
    if cleaned in {"", "-", ".", "-."}:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def extract_weight_from_name(name: object) -> float:
    if pd.isna(name):
        return 0.0
    match = _WEIGHT_IN_NAME_RE.search(str(name))
    if not match:
        return 0.0
    return to_float(match.group(1))


def resolve_columns(df: pd.DataFrame) -> dict[str, str]:
    available = {str(column).strip().lower(): str(column) for column in df.columns}
    resolved: dict[str, str] = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            key = alias.strip().lower()
            if key in available:
                resolved[canonical] = available[key]
                break
    return resolved


def normalize_input(df: pd.DataFrame, column_map: dict[str, str]) -> pd.DataFrame:
    normalized = df.copy()
    for canonical, source in column_map.items():
        normalized[canonical] = normalized[source]

    if "weight_g" not in column_map:
        normalized["weight_g"] = normalized["name"].apply(extract_weight_from_name)

    return normalized


def compute_kbzhu_tags(row: pd.Series) -> list[str]:
    calories = to_float(row["calories"])
    protein = to_float(row["protein"])
    fat = to_float(row["fat"])
    carbs = to_float(row["carbs"])
    weight_g = to_float(row["weight_g"])
    if weight_g <= 0:
        weight_g = 100.0

    category = normalize_text(row["category"])
    is_excluded_category = ("соус" in category) or ("напит" in category)
    calories_per_portion = (calories / 100.0) * weight_g

    tags: list[str] = []
    if calories_per_portion <= 200 and not is_excluded_category:
        tags.append("мало_калорий")
    if protein >= 15:
        tags.append("много_белка")
    if fat <= 5:
        tags.append("мало_жиров")
    if carbs >= 25 and fat <= 5:
        tags.append("много_углеводов")
    return tags


def compute_satiety_tag(row: pd.Series) -> str:
    protein = to_float(row["protein"])
    carbs = to_float(row["carbs"])
    fat = to_float(row["fat"])
    satiety_index = (protein * 3) + (carbs * 0.4) - (fat * 0.3)

    if satiety_index <= 20:
        return "перекус"
    if satiety_index <= 50:
        return "легкое"
    return "сытное"


def compute_cuisine_tags(name: object) -> list[str]:
    normalized_name = normalize_text(name)
    if not normalized_name:
        return []

    tags: list[str] = []
    for tag, keywords in CUISINE_KEYWORDS.items():
        if any(keyword in normalized_name for keyword in keywords):
            tags.append(tag)
    return tags


def validate_columns(df: pd.DataFrame) -> None:
    missing = [column for column in MANDATORY_COLUMNS if column not in df.columns]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"Во входном файле отсутствуют обязательные колонки: {missing_str}")


def add_tags(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    total_rows = len(df)
    issued_tags_total = 0

    tags_kbzhu: list[str] = []
    tags_satiety: list[str] = []
    tags_cuisine: list[str] = []
    tags_ingredients: list[str] = []

    for processed_rows, (_, row) in enumerate(df.iterrows(), start=1):
        kbzhu_tags = compute_kbzhu_tags(row)
        satiety_tag = compute_satiety_tag(row)
        cuisine_tags = compute_cuisine_tags(row["name"])

        tags_kbzhu.append(",".join(kbzhu_tags))
        tags_satiety.append(satiety_tag)
        tags_cuisine.append(",".join(cuisine_tags))
        tags_ingredients.append("")

        issued_tags_total += len(kbzhu_tags) + len(cuisine_tags) + (1 if satiety_tag else 0)
        if processed_rows % 100 == 0 or processed_rows == total_rows:
            print(
                f"Обработано строк: {processed_rows}/{total_rows}. "
                f"Выдано тегов: {issued_tags_total}"
            )

    result = df.copy()
    result["tags_kbzhu"] = tags_kbzhu
    result["tags_satiety"] = tags_satiety
    result["tags_cuisine"] = tags_cuisine
    result["tags_ingredients"] = tags_ingredients
    return result, issued_tags_total


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"Входной файл не найден: {input_path}")

    print(f"Загрузка файла: {input_path}")
    source_df = pd.read_excel(input_path)
    column_map = resolve_columns(source_df)
    df = normalize_input(source_df, column_map)
    validate_columns(df)
    print(f"Всего строк во входном файле: {len(df)}")
    print(
        "Маппинг колонок: "
        + ", ".join(
            f"{canonical}={column_map.get(canonical, 'fallback')}" for canonical in REQUIRED_COLUMNS
        )
    )

    tagged_df, issued_tags_total = add_tags(df)

    result_df = source_df.copy()
    result_df["tags_kbzhu"] = tagged_df["tags_kbzhu"]
    result_df["tags_satiety"] = tagged_df["tags_satiety"]
    result_df["tags_cuisine"] = tagged_df["tags_cuisine"]
    result_df["tags_ingredients"] = tagged_df["tags_ingredients"]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_excel(output_path, index=False)

    print(f"Готово. Обработано строк: {len(df)}")
    print(f"Всего выдано тегов: {issued_tags_total}")
    print(f"Файл результата: {output_path}")


if __name__ == "__main__":
    main()
