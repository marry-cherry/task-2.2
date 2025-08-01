import pandas as pd
from sqlalchemy import create_engine

# Подключение к БД
engine = create_engine("postgresql+psycopg2://postgres:LinUb20@localhost:5432/dwh")

# Загрузка CSV
csv_deal = pd.read_csv("deal_info.csv", encoding="cp1251", parse_dates=["effective_from_date"])
csv_product = pd.read_csv("product_info.csv", encoding="cp1251", parse_dates=["effective_from_date"])

# Загрузка из БД
db_deal = pd.read_sql("SELECT * FROM rd.deal_info", engine, parse_dates=["effective_from_date"])
db_product = pd.read_sql("SELECT * FROM rd.product", engine, parse_dates=["effective_from_date"])

def find_missing_dates(csv_df, db_df, table_name):
    print(f"\n--- {table_name} ---")

    # Приведение к datetime на всякий случай
    csv_df["effective_from_date"] = pd.to_datetime(csv_df["effective_from_date"], errors="coerce")
    db_df["effective_from_date"] = pd.to_datetime(db_df["effective_from_date"], errors="coerce")

    # Получение уникальных дат
    csv_dates = set(csv_df["effective_from_date"].dropna().unique())
    db_dates = set(db_df["effective_from_date"].dropna().unique())

    # Поиск отсутствующих
    missing = sorted(csv_dates - db_dates)

    if missing:
        print(f"Отсутствующие effective_from_date в БД:")
        for date in missing:
            print(f" - {date.date()}")
    else:
        print("Все значения effective_from_date из CSV уже есть в БД.")

# Поиск
find_missing_dates(csv_deal, db_deal, "deal_info")
find_missing_dates(csv_product, db_product, "product_info")
