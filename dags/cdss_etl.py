import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict
import pandas as pd
import numpy as np
import json
from array import array
from pandas import DataFrame
from airflow.models.dag import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from google.protobuf import json_format

# s3 upload requires boto dependency
import boto3
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
AWS_BUCKET_REGION = os.getenv('AWS_S3_REGION')

s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY, region_name=AWS_BUCKET_REGION)


import trackTypeRows_pb2

POSTGRES_CONN_ID = 'postgres-mg-etl'



@task
def upload_to_minio(local_file, s3_file):
    """
    Uploads a file into a S3 bucket.
    :param local_file: local file to upload to S3.
    :param s3_file: file path in the s3 bucket.
    :return: True if upload was successful.
    """

    logging.warn("Uploading file " + s3_file)
    try:
        s3.upload_file(local_file, AWS_BUCKET_NAME, s3_file)
        logging.warn(s3_file + " uploaded successfully")
        return True
    except FileNotFoundError:
        logging.error(s3_file + " file was not found")
        return False
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False

def export_vital_file(device_id: str, case_id: str):
    logging.warn("export_vital_file  " + device_id + case_id)

    hook1 = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_stmt1 = (
        "select name from device"
        f" where id = '{device_id}'::UUID;"
    )
    logging.warn(sql_stmt1)
    df1 = hook1.get_pandas_df(sql=sql_stmt1)
    name = df1.iloc[0,0]

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_stmt = (
        "select type, name, format, unit, srate, json_agg(ts order by ts asc) as ts, json_agg(val) as val"
        " from track_per_caseid_type"
        f" where device_id = '{device_id}'::UUID"
        f" and case_id= '{case_id}'"
        " group by type, name, format, unit, srate;"
    )
    logging.warn(sql_stmt)
    df = hook.get_pandas_df(sql=sql_stmt)

    print('df count: ', df['ts'].count(), df['ts'].count())
    if df['ts'].count() > 0:
        start_ts_list = df.iloc[0,5]
    else:
        return

    if isinstance(start_ts_list, list):
        start_ts = start_ts_list[0]
    else:
        start_ts = start_ts_list

    print('start_ts', start_ts)
    ts = datetime.fromtimestamp(int(start_ts)/1000)
    start_str = ts.strftime('%Y%m%d_%H%M%S')

    track_type_rows_pb = trackTypeRows_pb2.TrackTypeRows()
    for (idx, row_series) in df.iterrows():
        # print('Row Index label : ', idx)
        df_p = pd.DataFrame({'val': row_series['val']})
        df.at[idx , 'val'] =  np.concatenate(df_p['val'].values)
        track_type_row_pb = track_type_rows_pb.TrackTypeRow()
        tag_pb = track_type_rows_pb.Tag()
        tag_pb.key = ''
        tag_pb.value = ''

        track_type_row_pb.type = row_series['type']

        for ts in row_series['ts']:
            track_type_row_pb.ts.append(ts)

        for v in df.at[idx , 'val']:
            value_pb = track_type_row_pb.Value()
            value_pb.v.append(v)
            track_type_row_pb.val.append(value_pb)

        track_type_rows_pb.tag.append(tag_pb)
        track_type_rows_pb.track.append(track_type_row_pb)
    # make protobuf
    f = open(f'{start_str}_{name}.pb', "wb")
    f.write(track_type_rows_pb.SerializeToString())
    f.close()

def test_normalize():
    arr2: str = "[[60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60]]"
    "[[23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94]]"

    json_array2 = json.loads(arr2)
    df_p = pd.DataFrame({'val': json_array2})
    ft = np.concatenate(df_p['val'].values)

def get_device_case_names():
    # logging.warn('get_device_case_names')

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_stmt = (
        "select device_id, case_id"
        " from caseid_start_to_end;"
    )
    # logging.warn(sql_stmt)
    df = hook.get_pandas_df(sql=sql_stmt)
    return df

def manual_pb_gen():
    df = get_device_case_names()
    # [1673 rows x 2 columns]
    arr: np.ndarray = df.to_numpy()
    cases =  list(arr)

    for case in cases:
        if case[0] is  None or case[1] is None:
            continue
        export_vital_file(case[0], case[1])


if __name__ == "__main__":
    print('start test')
    # export_vital_file('130bc450-406b-11ed-8f01-cfef1303339c', '007832df11')
    # test_normalize()
    manual_pb_gen()
    print('finish main')





def generate_task(**args):
    # logging.info(args)
    device_id:str = args['device_id']
    case_id:str = args['case_id']
    idx:str = args['idx']
    # logging.warn("generate_tasks " + device_id + case_id)
    export_task = PythonOperator(
       task_id=f"'task_export_vital_file_{idx}'",
       python_callable=export_vital_file,
       op_args='{"device_id": {device_id}, "case_id": {case_id}}',
       dag=dag,
    )
    return export_task

def task_upload_vital_files(**context):
    dag_instance = context['dag']
    context = get_current_context()
    df = get_device_case_names()
    # [1673 rows x 2 columns]
    arr: np.ndarray = df.to_numpy()
    cases =  list(arr)
    # logging.warn('length: ' + str(len(cases)))

    tasks = []
    idx = 0
    for case in cases:
        idx += 101
        if case[0] is  None or case[1] is None:
            continue
        device_id: str = case[0]
        case_id: str = case[1]
        # logging.info('device_id: ' + str(device_id) + ' case_id: ' + str(case_id))
        task = generate_task(device_id=device_id, case_id=case_id, idx=idx, dag=dag_instance)
        tasks.append(task)

    # logging.info('tasks: ' + str(len(tasks)))
    return tasks


with DAG(
    dag_id="cdss_etl",
    # start_date=datetime(2022, 10, 13),
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=120),
    tags=["cdss"]
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("duplicate_tb", tooltip="Tasks for duplicate") as duplicate_tb:
        create_etl_meta = PostgresOperator(
            task_id='create_etl_meta',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''create table IF NOT EXISTS etl_meta
                (
                    device_id     uuid   not null
                        constraint etl_meta_pk
                            primary key,
                    last_start_ts bigint not null,
                    last_case_id  text   not null,
                    device_name   text,
                    last_end_ts   bigint not null
                );

                alter table etl_meta
                    owner to postgres;

                create index IF NOT EXISTS etl_meta_last_start_ts_index
                    on etl_meta (last_start_ts desc);

                create index IF NOT EXISTS etl_meta_last_end_ts_index
                    on etl_meta (last_end_ts desc);''',
            dag=dag,
        )

        duplicate_device = PostgresOperator(
            task_id='duplicate_device',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="drop table if exists device_{{ ds_nodash }}; select id, name into device_{{ ds_nodash }} from thingsboard.device;",
            dag=dag,
        )

        duplicate_ts_kv_dictionary = PostgresOperator(
            task_id='duplicate_ts_kv_dictionary',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="drop table if exists ts_kv_dictionary_{{ ds_nodash }};select key, key_id into ts_kv_dictionary_{{ ds_nodash }} from thingsboard.ts_kv_dictionary;",
            dag=dag,
        )

        duplicate_device >> duplicate_ts_kv_dictionary

    with TaskGroup("process_etl", tooltip="Tasks for ETL") as process_etl:
        generate_raw_track = PostgresOperator(
            task_id='generate_raw_track',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists raw_track_{{ ds_nodash }};
                    select entity_id as device_id, ts, json_v as track_data
                    into raw_track_{{ ds_nodash }}
                    from thingsboard.ts_kv
                    where key = (
                        select key_id
                        from ts_kv_dictionary_{{ ds_nodash }}
                        where key = 'tracks'
                        )
                    and ts > (select case when max(last_start_ts) > 1 then max(last_start_ts) else 1648738800000 end last_ts from etl_meta);''',
            dag=dag,
        )

        generate_caseid_start_to_end = PostgresOperator(
            task_id='generate_caseid_start_to_end',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists caseid_start_to_end_{{ ds_nodash }};
                    select entity_id as device_id, str_v as case_id, min(ts) as start_ts, max(ts) as end_ts
                    into caseid_start_to_end_{{ ds_nodash }}
                        from thingsboard.ts_kv
                        where key = (select key_id
                                    from ts_kv_dictionary
                                    where key = 'caseid')
                            and ts between (select min(ts) as start_ts from raw_track) and (select max(ts) as end_ts from raw_track)
                        group by entity_id, case_id;''',
            dag=dag,
        )

        generate_track_per_type = PostgresOperator(
            task_id='generate_track_per_type',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists track_per_type_{{ ds_nodash }};
                    select device_id,
                    json_array_elements(json_v)->>'type' as type,
                    json_array_elements(json_v)->>'name' as name,
                    json_array_elements(json_v)->>'format' as format,
                    json_array_elements(json_v)->>'unit' as unit,
                    json_array_elements(json_v)->>'srate' as srate,
                    floor((json_array_elements(json_array_elements(json_v)->'data')->>'ts')::float * 1000)::bigint as ts,
                    json_array_elements(json_array_elements(json_v)->'data')->'val' as val
                into track_per_type_{{ ds_nodash }}
                from raw_track;''',
            dag=dag,
        )

        generate_track_per_caseid_type = PostgresOperator(
            task_id='generate_track_per_caseid_type',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists track_per_caseid_type_{{ ds_nodash }};
                select caseid.case_id, caseid.start_ts, caseid.end_ts, track.*
                into track_per_caseid_type_{{ ds_nodash }}
                from caseid_start_to_end as caseid
                join track_per_type as track
                on caseid.device_id = track.device_id
                and track.ts between caseid.start_ts and caseid.end_ts;''',
            dag=dag,
        )

        generate_raw_track >> generate_caseid_start_to_end >> generate_track_per_type >> generate_track_per_caseid_type

    with TaskGroup("upload_vital_file", tooltip="Tasks for update upload_vital_file") as upload_vital_file:
        start_export = DummyOperator(task_id="start_export")

        export_vital = PythonOperator(
            task_id='export_vital',
            python_callable=task_upload_vital_files,
            dag=dag,
        )

        start_export >> export_vital

    with TaskGroup("refresh_meta", tooltip="Tasks for update ETL_META") as refresh_meta:
        update_meta = PostgresOperator(
            task_id='update_meta',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''UPDATE etl_meta em
                    set last_case_id = d.case_id,
                    last_start_ts = d.start_ts,
                    last_end_ts = d.end_ts
                FROM device_track as d
                WHERE em.device_id = d.device_id;''',
            dag=dag,
        )

        insert_meta = PostgresOperator(
            task_id='insert_meta',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''INSERT INTO  etl_meta(device_id, last_case_id, last_start_ts, last_end_ts, device_name)
                    select device_id, case_id as last_case_id, start_ts as last_start_ts, end_ts as last_end_ts, d.name as device_name
                    from (select device_id, case_id, start_ts, end_ts, rank() OVER (PARTITION BY device_id ORDER BY start_ts DESC) as rk
                        from device_track
                        where device_id not in (select device_id from etl_meta)
                        group by device_id, case_id, start_ts, end_ts
                        order by device_id, start_ts desc) as dt
                        join thingsboard.device as d on dt.device_id = d.id
                    where rk = 1;''',
            dag=dag,
        )

        update_meta >> insert_meta

    end = DummyOperator(task_id="end")

    # start >> process_etl >> duplicate_tb  >> upload_vital_file >> refresh_meta >> end
    start >> process_etl >> duplicate_tb  >> refresh_meta >> end
    # start >> upload_vital_file >> refresh_meta >> end

