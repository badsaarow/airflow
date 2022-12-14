import logging
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
from array import array
from pandas import DataFrame
from airflow.models.dag import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

POSTGRES_CONN_ID = 'postgres-gt-etl'
TB_SCHEMA = 'public'
ETC_SCHEMA = 'tb_ext'
SYNC_DATE = '20221023'

if __name__ == "__main__":
    print('start test')
    # export_vital_file('130bc450-406b-11ed-8f01-cfef1303339c', '007832df11')
    # test_normalize()
    # print(is_uploaded('20220425_205719'))
    print('finish main')

with DAG(
    dag_id="water_etl",
    start_date=days_ago(1),
    schedule_interval=timedelta(hours=24),
    tags=["water"]
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("duplicate_tb", tooltip="Tasks for duplicate") as duplicate_tb:
        create_etl_meta = PostgresOperator(
            task_id='create_etl_meta',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''create table IF NOT EXISTS {{ETC_SCHEMA}}.etl_meta
                (
                    device_id     uuid   not null
                        constraint etl_meta_pk
                            primary key,
                    device_name   text,
                    last_ts   bigint not null
                );
                alter {{ETC_SCHEMA}}.table etl_meta
                    owner to postgres;
                create index IF NOT EXISTS {{ETC_SCHEMA}}.etl_meta_last_ts_index
                    on {{ETC_SCHEMA}}.etl_meta (last_ts desc);
                create index IF NOT EXISTS {{ETC_SCHEMA}}.etl_metaindex
                    on {{ETC_SCHEMA}}.etl_meta desc);''',
            dag=dag,
        )

        duplicate_device = PostgresOperator(
            task_id='duplicate_device',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="drop table if exists {{ETC_SCHEMA}}.device_{{ ds_nodash }}; select id, name into {{ETC_SCHEMA}}.device_{{ ds_nodash }} from {{ TB_SCHEMA }}.device;",
            dag=dag,
        )

        duplicate_ts_kv_dictionary = PostgresOperator(
            task_id='duplicate_ts_kv_dictionary',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="drop table if exists {{ETC_SCHEMA}}.ts_kv_dictionary_{{ ds_nodash }};select key, key_id into {{ETC_SCHEMA}}.ts_kv_dictionary_{{ ds_nodash }} from {{ TB_SCHEMA }}.ts_kv_dictionary;",
            dag=dag,
        )

        create_etl_meta >> duplicate_device >> duplicate_ts_kv_dictionary

    with TaskGroup("process_etl", tooltip="Tasks for ETL") as process_etl:
        generate_raw_meter = PostgresOperator(
            task_id='generate_raw_meter',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists {{ETC_SCHEMA}}.raw_meter_{{ ds_nodash }};
                    select entity_id as device_id, ts, long_v as meter_serial_no
                    into {{ETC_SCHEMA}}.raw_meter_{{ ds_nodash }}
                    from {{ TB_SCHEMA }}.ts_kv
                    where key = (
                        select key_id
                        from {{ETC_SCHEMA}}.ts_kv_dictionary_{{ ds_nodash }}
                        where key = 'meter_serial_no'
                        )
                    and ts > (select case when max(last_ts) > 1 then max(last_ts) else 1648738800000 end last_ts from etl_meta);''',
            dag=dag,
        )

        generate_track_per_type = PostgresOperator(
            task_id='generate_track_per_type',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists {{ETC_SCHEMA}}.track_per_type_{{ ds_nodash }};
                    select device_id,
                    json_array_elements(track_data)->>'type' as type,
                    json_array_elements(track_data)->>'name' as name,
                    json_array_elements(track_data)->>'format' as format,
                    json_array_elements(track_data)->>'unit' as unit,
                    json_array_elements(track_data)->>'srate' as srate,
                    floor((json_array_elements(json_array_elements(track_data)->'data')->>'ts')::float * 1000)::bigint as ts,
                    json_array_elements(json_array_elements(track_data)->'data')->'val' as val
                into {{ETC_SCHEMA}}.track_per_type_{{ ds_nodash }}
                from {{ETC_SCHEMA}}.raw_meter_{{ ds_nodash }};''',
            dag=dag,
        )

        generate_track_per_caseid_type = PostgresOperator(
            task_id='generate_track_per_caseid_type',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists {{ETC_SCHEMA}}.track_per_caseid_type_{{ ds_nodash }};
                select caseid.case_id, caseid.start_ts, caseid.end_ts, track.*
                into {{ETC_SCHEMA}}.track_per_caseid_type_{{ ds_nodash }}
                from {{ETC_SCHEMA}}.caseid_start_to_end_{{ ds_nodash }} as caseid
                    join track_per_type_{{ ds_nodash }} as track
                    on caseid.device_id = track.device_id
                    and track.ts between caseid.start_ts and caseid.end_ts;
                create index {{ETC_SCHEMA}}.track_per_caseid_type_{{ ds_nodash }}_export
                    on {{ETC_SCHEMA}}.track_per_caseid_type_{{ ds_nodash }}
                    (device_id, case_id, type, name, format, unit, srate, ts);
                ''',
            dag=dag,
        )


        generate_device_case = PostgresOperator(
            task_id='generate_device_case',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists {{ETC_SCHEMA}}.device_case_{{ ds_nodash }};
                    select device_id, case_id, min(ts) as start_ts, max(ts) as end_ts
                    into {{ETC_SCHEMA}}.device_case_{{ ds_nodash }}
                    from {{ETC_SCHEMA}}.track_per_caseid_type_{{ ds_nodash }}
                    group by device_id, case_id;''',
            dag=dag,
        )

        generate_raw_meter >> generate_caseid_start_to_end >> generate_track_per_type >> generate_track_per_caseid_type

    with TaskGroup("refresh_meta", tooltip="Tasks for update ETL_META") as refresh_meta:
        update_meta = PostgresOperator(
            task_id='update_meta',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''UPDATE {{ETC_SCHEMA}}.etl_meta em
                 set last_case_id = d.case_id,
                 last_ts = d.start_ts,
                 FROM {{ETC_SCHEMA}}.device_case_{SYNC_DATE} as d
                 WHERE em.device_id = d.device_id;''',
            dag=dag,
        )

        insert_meta = PostgresOperator(
            task_id='insert_meta',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''INSERT INTO  {{ETC_SCHEMA}}.etl_meta(device_id, last_case_id, last_ts, device_name)
                    select device_id, case_id as last_case_id, start_ts as last_ts, end_ts as d.name as device_name
                    from (select device_id, case_id, start_ts, end_ts, rank() OVER (PARTITION BY device_id ORDER BY start_ts DESC) as rk
                        from {{ETC_SCHEMA}}.device_case_{{ ds_nodash }}
                        where device_id not in (select device_id from etl_meta)
                        group by device_id, case_id, start_ts, end_ts
                        order by device_id, start_ts desc) as dt
                        join {{ETC_SCHEMA}}.device_{{ ds_nodash }} as d on dt.device_id = d.id
                    where rk = 1;''',
            dag=dag,
        )

        update_meta >> insert_meta

    end = EmptyOperator(task_id="end")

    start >> process_etl >> duplicate_tb  >> upload_vital_file >> refresh_meta >> end
