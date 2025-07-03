import os
import logging
from datetime import datetime

import boto3
import pandas as pd
import vertica_python

from airflow.decorators import dag, task


# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Параметры S3
AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"
S3_ENDPOINT_URL = "https://storage.yandexcloud.net"
BUCKET_NAME = "sprint6"
FILE_NAME = "group_log.csv"
LOCAL_PATH = "/data"

# Параметры Vertica
VERTICA_CONN_INFO = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv202506163',
    'password': 'G0yiOsXkrU1XkGV',
    'database': 'dwh',
    'autocommit': True
}
VERTICA_SCHEMA = "STV202506163__STAGING"
VERTICA_TABLE = "group_log"


@dag(
    schedule_interval=None,
    start_date=datetime(2025, 7, 3),
    catchup=False,
    tags=["sprint6", "project"]
)
def load_group_log_to_vertica_dag():

    @task
    def download_file_from_s3():
        """Скачиваем group_log.csv из S3"""
        try:
            os.makedirs(LOCAL_PATH, exist_ok=True)
            local_file = os.path.join(LOCAL_PATH, FILE_NAME)

            logger.info("Подключаемся к S3...")
            session = boto3.session.Session()
            s3 = session.client(
                service_name='s3',
                endpoint_url=S3_ENDPOINT_URL,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )

            logger.info("Скачиваем файл %s из ведра %s",
                        FILE_NAME, BUCKET_NAME)
            s3.download_file(BUCKET_NAME, FILE_NAME, local_file)
            logger.info("Файл %s успешно скачан в %s",
                        FILE_NAME, local_file)

        except Exception as e:
            logger.error("Ошибка при скачивании из S3: %s",
                         str(e), exc_info=True)
            raise

    @task
    def load_file_to_vertica():
        """Загружаем group_log.csv в Vertica"""
        local_file = os.path.join(LOCAL_PATH, FILE_NAME)
        try:
            logger.info("Читаем файл %s", local_file)
            df = pd.read_csv(local_file)

            # Приведение типов
            df["user_id_from"] = pd.array(df["user_id_from"], dtype="Int64")
            df["datetime"] = pd.to_datetime(df["datetime"])

            # Переименование столбца под структуру таблицы
            df.rename(columns={"datetime": "event_datetime"}, inplace=True)

            logger.info("Успешно прочитано строк: %d", len(df))

            # Формируем соединение
            conn = vertica_python.connect(**VERTICA_CONN_INFO)

            # Загружаем в таблицу
            copy_sql = f"""
            COPY {VERTICA_SCHEMA}.{VERTICA_TABLE}
            (group_id, user_id, user_id_from, event, event_datetime)
            FROM STDIN DELIMITER ',' ENCLOSED BY '\"' SKIP 1;
            """

            with conn.cursor() as cur:
                # Переводим DataFrame в CSV в память
                csv_data = df.to_csv(index=False, header=False, quoting=1)

                logger.info("Начинаем загрузку в Vertica...")
                cur.copy(copy_sql, csv_data)
                conn.commit()
                logger.info("Загрузка завершена успешно.")

            conn.close()

        except Exception as e:
            logger.error("Ошибка при загрузке в Vertica: %s",
                         str(e), exc_info=True)
            raise

    # Определение последовательности задач
    downloaded = download_file_from_s3()
    load_file_to_vertica().set_upstream(downloaded)


dag = load_group_log_to_vertica_dag()
