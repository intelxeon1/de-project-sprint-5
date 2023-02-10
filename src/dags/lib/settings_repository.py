from typing import Dict, Optional

from psycopg import Connection, sql
from psycopg.rows import class_row
from pydantic import BaseModel


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class EtlSettingsRepository:
    def __init__(self, schema:str):
        self._schema = schema
        
    def get_setting(self, conn: Connection,etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(sql.SQL(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM {}.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """).format(sql.Identifier(self._schema)),
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(sql.SQL(
                """
                    INSERT INTO {}.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """).format(sql.Identifier(self._schema)),
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )
