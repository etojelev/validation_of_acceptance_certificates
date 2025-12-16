import io
import json
import re
import zipfile
from datetime import date, datetime
from logging import getLogger
from pathlib import Path
from typing import Any

import pandas as pd

logger = getLogger(__name__)


def get_tokens() -> Any:
    tokens_path = Path(__file__).parents[2] / "tokens.json"
    with tokens_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _extract_date_from_df(df: pd.DataFrame) -> date | None:
    try:
        date_cell = df.iloc[2, 3]

        if pd.isna(date_cell):
            logger.warning("Ячейка с датой пуста")
            return None

        date_str = str(date_cell).strip()

        date_str = re.sub(r"\s*г\.?$", "", date_str)

        date_formats = [
            "%d.%m.%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d.%m.%y",
        ]

        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt).date()
                logger.debug(f"Дата успешно распарсена: {parsed_date} (формат: {fmt})")
                return parsed_date
            except ValueError:
                continue
        logger.warning(f"Не удалось распарсить дату из строки: '{date_str}'")
        return None
    except Exception as e:
        logger.error(f"Ошибка извлечения даты: {e}")
        return None


def _process_excel_data_simple(df: pd.DataFrame) -> list[dict[str, Any]]:
    processed_data = []

    data_rows = df.iloc[12:] if len(df) > 10 else pd.DataFrame()

    for _, row in data_rows.iterrows():
        if pd.isna(row.iloc[3]) or str(row.iloc[3]) == "Итого":
            continue

        try:
            item = {
                "order_id": int(float(row.iloc[1]))
                if not pd.isna(row.iloc[1])
                else None,
                "sticker": int(float(row.iloc[3]))
                if not pd.isna(row.iloc[3])
                else None,
                "count": int(float(row.iloc[4])) if not pd.isna(row.iloc[4]) else None,
            }

            if item["order_id"] and item["sticker"]:
                processed_data.append(item)

        except (ValueError, TypeError) as error:
            logger.warning(f"Невозможно обработать строку {row.tolist()}: {error}")
            continue

    return processed_data


def extract_excel_from_zip(
    archive_bytes: bytes, path: str = ""
) -> list[dict[str, Any]]:
    all_data = []

    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            for file_name in archive.namelist():
                file_path = f"{path}/{file_name}" if path else file_name

                if file_name.endswith(".zip"):
                    with archive.open(file_name) as nested_file:
                        nested_bytes = nested_file.read()
                        nested_data = extract_excel_from_zip(nested_bytes, file_path)
                        all_data.extend(nested_data)

                elif file_name.endswith((".xlsx", ".xls", ".xlsm")):
                    try:
                        with archive.open(file_name) as excel_file:
                            excel_bytes = excel_file.read()

                            df = pd.read_excel(
                                io.BytesIO(excel_bytes), engine="openpyxl", header=None
                            )

                            data = _process_excel_data_simple(df)
                            file_date = _extract_date_from_df(df)

                            supply_id = (
                                f"WB-GI-{file_name.split('.')[0].split('-')[-1]}"
                            )

                            if data:
                                all_data.append(
                                    {
                                        "supply_id": supply_id,
                                        "date": file_date.isoformat()
                                        if file_date
                                        else None,
                                        "data": data,
                                    }
                                )

                    except Exception as error:
                        logger.error(f"Ошибка парсинга файла: {file_path}: {error}")

    except zipfile.BadZipFile:
        logger.error(f"Некорректный zip архив: {path}")
    except Exception as error:
        logger.error(f"Ошибока обработки архива {path}: {error}")

    return all_data
