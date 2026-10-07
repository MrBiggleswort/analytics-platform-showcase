from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ClinicConfig(BaseModel):
    """Набор клиник внутри одного тенанта."""

    model_config = ConfigDict(extra="forbid")

    clinic_external_id: int = Field(gt=0)
    clinic_code: str
    timezone: str = "Europe/Moscow"


class CsvFeedConfig(BaseModel):
    """Одна файловая выгрузка, привязанная к клинике тенанта."""

    model_config = ConfigDict(extra="forbid")

    clinic_external_id: int = Field(gt=0)
    folder: str


class HttpTriggerConfig(BaseModel):
    """Триггер управляемого получения данных без публикации реального адреса."""

    model_config = ConfigDict(extra="forbid")

    method: Literal["GET", "POST"] = "POST"
    timeout_seconds: int = Field(default=30, ge=1, le=300)


class AcquisitionConfig(BaseModel):
    """Описывает, как входные данные становятся готовыми до запуска преобразований."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["managed", "external"] = "managed"
    poll_interval_seconds: int = Field(default=60, ge=5, le=300)
    csv_stability_seconds: int = Field(default=30, ge=0, le=600)
    trigger: HttpTriggerConfig | None = None


class CsvConfig(BaseModel):
    """CSV-источник для одной или нескольких файловых выгрузок."""

    model_config = ConfigDict(extra="forbid")

    feeds: list[CsvFeedConfig]
    delimiter: str = ";"
    encoding: str = "utf-8-sig"

    @field_validator("delimiter")
    @classmethod
    def validate_delimiter(cls, value: str) -> str:
        if len(value) != 1:
            raise ValueError("delimiter must contain exactly one character")
        return value


class ClientConfig(BaseModel):
    """Единый контракт конфигурации одного клиента."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: int = Field(gt=0)
    tenant_code: str
    source_type: Literal["csv", "firebird", "rest_api", "onec_api"]
    clinics: list[ClinicConfig]
    acquisition: AcquisitionConfig = Field(default_factory=AcquisitionConfig)
    csv: CsvConfig | None = None

    @model_validator(mode="after")
    def validate_runtime_contract(self):
        clinic_ids = [clinic.clinic_external_id for clinic in self.clinics]

        if not clinic_ids:
            raise ValueError("clinics must contain at least one clinic")

        if len(clinic_ids) != len(set(clinic_ids)):
            raise ValueError("clinics contains duplicate clinic_external_id")

        if self.source_type == "csv":
            if self.csv is None:
                raise ValueError("csv source requires csv config")

            feed_ids = {feed.clinic_external_id for feed in self.csv.feeds}
            unknown = sorted(feed_ids - set(clinic_ids))
            if unknown:
                raise ValueError("csv feeds must belong to client clinics")

            if self.acquisition.mode == "managed" and self.acquisition.trigger is None:
                raise ValueError("managed csv acquisition requires trigger")

            if self.acquisition.mode == "external" and self.acquisition.trigger is not None:
                raise ValueError("external csv acquisition must not define trigger")
        else:
            if self.acquisition.mode != "managed":
                raise ValueError("non-csv transports require managed acquisition")

            if self.acquisition.trigger is not None:
                raise ValueError("trigger is supported only for managed csv acquisition")

        return self


class ProjectConfig(BaseModel):
    """Полная конфигурация выполнения без рабочих значений инфраструктуры."""

    clients: list[ClientConfig]
