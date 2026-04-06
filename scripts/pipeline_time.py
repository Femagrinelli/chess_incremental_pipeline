import os

import pendulum


PIPELINE_TIMEZONE = os.environ.get("PIPELINE_TIMEZONE", "America/Sao_Paulo")


def airflow_timezone():
    return pendulum.timezone(PIPELINE_TIMEZONE)


def airflow_start_date():
    return pendulum.datetime(2024, 1, 1, tz=airflow_timezone())
