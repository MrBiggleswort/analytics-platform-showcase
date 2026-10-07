from __future__ import annotations

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine

from src.common.acquisition import (
    acquisition_for_tenant,
    finalize_wave,
    start_wave,
    wave_ready,
)
from src.pipeline.acquisition import acquire_tenant_for_wave
from src.pipeline.run_tenant_pipeline import (
    acquire_raw,
    continue_pipeline,
)
from src.sources.csv_source import CsvSourceClient
from src.sources.rest_source import RestSourceClient


# Рабочая конфигурация загружается из Pydantic-моделей.
# Здесь оставлены только нейтральные параметры двух типов источников.
TENANTS = (
    {
        "tenant_id": 1,
        "tenant_code": "tenant_a",
        "source_type": "csv",
        "source_system": "mis_csv",
        "acquisition_mode": "external",
        "csv_root_env": "TENANT_A_CSV_ROOT",
    },
    {
        "tenant_id": 2,
        "tenant_code": "tenant_b",
        "source_type": "api",
        "source_system": "mis_api",
        "acquisition_mode": "direct",
        "api_url_env": "TENANT_B_API_URL",
        "api_token_env": "TENANT_B_API_TOKEN",
    },
)


def get_engine():
    """Создает подключение SQLAlchemy из окружения без зашитых учетных данных."""

    dsn = os.environ["DWH_DATABASE_URL"]

    return create_engine(
        dsn,
        pool_pre_ping=True,
        future=True,
    )


def tenant_config(
    tenant_id: int,
) -> dict:
    for tenant in TENANTS:
        if int(tenant["tenant_id"]) == int(tenant_id):
            return tenant

    raise KeyError(
        f"Конфигурация тенанта не найдена: {tenant_id}"
    )


def build_source_client(
    tenant: dict,
):
    """Создает адаптер по `source_type` тенанта."""

    source_type = str(
        tenant["source_type"]
    )

    if source_type == "csv":
        root = os.environ[
            str(tenant["csv_root_env"])
        ]

        return CsvSourceClient(
            folder=root,
            file_map={
                "doctors": "doctors.csv",
                "patients": "patients.csv",
                "services": "services.csv",
                "visits": "visits.csv",
                "sales": "sales.csv",
                "sale_services":
                    "sale_services.csv",
                "visit_appointed_services":
                    "visit_appointed_services.csv",
                "visit_rendered_services":
                    "visit_rendered_services.csv",
            },
        )

    if source_type == "api":
        base_url = os.environ[
            str(tenant["api_url_env"])
        ]

        token = os.environ[
            str(tenant["api_token_env"])
        ]

        return RestSourceClient(
            base_url=base_url,
            token=token,
        )

    raise ValueError(
        f"Неподдерживаемый source_type: {source_type}"
    )


def acquire_raw_for_tenant(
    *,
    tenant_id: int,
    deadline,
):
    """Читает источник и фиксирует `raw`, не запуская последующую обработку."""

    if datetime.now(
        deadline.tzinfo
    ) >= deadline:
        raise TimeoutError(
            "Общий срок получения данных уже истек"
        )

    tenant = tenant_config(
        tenant_id
    )

    run_id = acquire_raw(
        get_engine(),
        tenant_id=tenant_id,
        source_system=str(
            tenant["source_system"]
        ),
        source=build_source_client(
            tenant
        ),
    )

    return run_id, {
        "source_type":
            tenant["source_type"],
        "deadline":
            deadline.isoformat(),
    }


def continue_tenant_pipeline(
    *,
    tenant_id: int,
    load_run_id: str,
):
    """Продолжает `stage`, `mart` и публикацию после барьера."""

    tenant = tenant_config(
        tenant_id
    )

    today = date.today()

    return continue_pipeline(
        get_engine(),
        tenant_id=tenant_id,
        source_system=str(
            tenant["source_system"]
        ),
        load_run_id=load_run_id,
        date_from=
            today - timedelta(days=45),
        date_to=today,
    )


def start_acquisition_wave(
    *,
    airflow_dag_id: str,
    airflow_run_id: str,
) -> str:
    engine = get_engine()

    tenants = [
        {
            "tenant_id":
                tenant["tenant_id"],
            "tenant_code":
                tenant["tenant_code"],
            "source_type":
                tenant["source_type"],
            "acquisition_mode":
                tenant["acquisition_mode"],
            "airflow_task_id":
                f"tenant_{tenant['tenant_id']}"
                ".acquire_input",
        }
        for tenant in TENANTS
    ]

    return start_wave(
        engine,
        tenants=tenants,
        timeout_minutes=120,
        airflow_dag_id=
            airflow_dag_id,
        airflow_run_id=
            airflow_run_id,
    )


def acquisition_wave_ready(
    wave_id: str,
) -> bool:
    return wave_ready(
        get_engine(),
        wave_id=wave_id,
    )


def acquire_tenant(
    *,
    tenant_id: int,
    wave_id: str,
    task_id: str,
) -> str:
    return acquire_tenant_for_wave(
        get_engine(),
        wave_id=wave_id,
        tenant_id=tenant_id,
        airflow_task_id=task_id,
        acquire_raw=
            acquire_raw_for_tenant,
    )


def finish_acquisition_wave(
    wave_id: str,
) -> dict:
    return finalize_wave(
        get_engine(),
        wave_id=wave_id,
    )


def process_acquired_tenant(
    *,
    tenant_id: int,
    wave_id: str,
) -> str:
    row = acquisition_for_tenant(
        get_engine(),
        wave_id=wave_id,
        tenant_id=tenant_id,
    )

    if (
        row is None
        or row["status"] != "success"
        or not row.get("load_run_id")
    ):
        status = (
            row["status"]
            if row is not None
            else "missing"
        )

        raise AirflowSkipException(
            f"Tenant {tenant_id} "
            "пропущен после получения данных: "
            f"status={status}"
        )

    return continue_tenant_pipeline(
        tenant_id=tenant_id,
        load_run_id=
            str(row["load_run_id"]),
    )


with DAG(
    dag_id="tenant_elt_showcase",
    description=(
        "Параллельное получение данных "
        "с общим барьером и "
        "последующей обработкой каждого тенанта"
    ),
    start_date=datetime(
        2025,
        1,
        1,
    ),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "dwh",
        "retries": 1,
        "retry_delay":
            timedelta(minutes=5),
    },
    tags=[
        "elt",
        "multi_tenant",
    ],
) as dag:

    start = PythonOperator(
        task_id=
            "start_acquisition_wave",
        python_callable=
            start_acquisition_wave,
        op_kwargs={
            "airflow_dag_id":
                "{{ dag.dag_id }}",
            "airflow_run_id":
                "{{ run_id }}",
        },
        retries=0,
    )

    wait_terminal = PythonSensor(
        task_id=
            "wait_for_terminal_states",
        python_callable=
            acquisition_wave_ready,
        op_kwargs={
            "wave_id":
                "{{ ti.xcom_pull("
                "task_ids="
                "'start_acquisition_wave'"
                ") }}",
        },
        mode="reschedule",
        poke_interval=30,
        timeout=7500,
        retries=0,
    )

    barrier = PythonOperator(
        task_id="acquisition_barrier",
        python_callable=
            finish_acquisition_wave,
        op_kwargs={
            "wave_id":
                "{{ ti.xcom_pull("
                "task_ids="
                "'start_acquisition_wave'"
                ") }}",
        },
        retries=0,
    )

    start >> wait_terminal >> barrier

    for tenant in TENANTS:
        tenant_id = int(
            tenant["tenant_id"]
        )

        group_id = (
            f"tenant_{tenant_id}"
        )

        with TaskGroup(
            group_id=group_id,
        ):
            acquire = PythonOperator(
                task_id="acquire_input",
                python_callable=
                    acquire_tenant,
                op_kwargs={
                    "tenant_id":
                        tenant_id,
                    "wave_id":
                        "{{ ti.xcom_pull("
                        "task_ids="
                        "'start_acquisition_wave'"
                        ") }}",
                    "task_id":
                        f"{group_id}"
                        ".acquire_input",
                },
                retries=0,
                execution_timeout=
                    timedelta(
                        minutes=121
                    ),
            )

            process = PythonOperator(
                task_id=
                    "process_acquired",
                python_callable=
                    process_acquired_tenant,
                op_kwargs={
                    "tenant_id":
                        tenant_id,
                    "wave_id":
                        "{{ ti.xcom_pull("
                        "task_ids="
                        "'start_acquisition_wave'"
                        ") }}",
                },
                retries=0,
            )

        start >> acquire
        barrier >> process
