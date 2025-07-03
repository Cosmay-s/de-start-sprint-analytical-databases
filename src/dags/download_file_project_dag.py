import os
import boto3
import logging
from datetime import datetime
from airflow.decorators import dag, task


logger = logging.getLogger(__name__)


AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"
S3_ENDPOINT_URL = "https://storage.yandexcloud.net"
BUCKET_NAME = "sprint6"
FILE_NAME = "group_log.csv"
LOCAL_PATH = "/data"


@dag(
    schedule_interval=None,
    start_date=datetime(2025, 7, 3),
    catchup=False,
    tags=["sprint6", "project"]
)
def download_group_log_dag():

    @task
    def download_file_from_s3():
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

            logger.info("Начинаем скачивание файла %s из бакета %s",
                        FILE_NAME, BUCKET_NAME)
            s3.download_file(BUCKET_NAME, FILE_NAME, local_file)
            logger.info("Файл %s успешно скачан в %s", FILE_NAME, local_file)

        except Exception as e:
            logger.error("Ошибка при скачивании файла из S3: %s",
                         str(e), exc_info=True)
            raise

    download_file_from_s3()


dag = download_group_log_dag()

