import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict
import pandas as pd
import numpy as np
import json
import re
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
from minio import Minio
from minio.error import S3Error

ACCESS_KEY = 'ysKyN2stJ0a53uqo'
SECRET_KEY = 'wRBfUxNBcP5SZTpeNBpehVA85gZjrQba'
BUCKET_NAME = 'cdss-bucket'
BUCKET_REGION = 'ap-northeast-2'

minio_client = Minio(
        "appwrite.stg.zt1.huinnoaim.com:9000",
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

import trackTypeRows_pb2

POSTGRES_CONN_ID = 'postgres-mg-etl'
PROTO_FILE = 'trackTypeRows.proto'
EUMC_MIG_DATE = '20221023'

def upload_to_minio(local_file, s3_file):
    logging.warning("Uploading file " + s3_file)
    try:
        minio_client.fput_object(BUCKET_NAME, s3_file, local_file)
        logging.warning(s3_file + " uploaded successfully")
        return True
    except S3Error as ex:
        logging.error(s3_file, ex)
        return False

def get_uploaded():
    return pd.read_csv('dags/uploaded.txt')

def is_uploaded(str_ts: str):
    df = get_uploaded()
    print('upload: ' + str(len(df)))
    df_uploaded = df.query(f"datetimestring == '{str_ts}'")
    if df_uploaded is None:
        return False
    return True

def mk_tgz(src: str):
    logging.debug('mk_tgz: ' + src)
    cmd = (
        f"rm -rf /tmp/{src} "
        f"&& mkdir -p /tmp/{src} "
        f"&& cp {PROTO_FILE} /tmp/{src}/ "
        f"&& mv {src}.pb /tmp/{src}/ "
        f"&& cd /tmp/{src}/ "
        f"&& tar -czvf {src}.tgz {PROTO_FILE} {src}.pb"
    )
    print(cmd)
    os.system(cmd)

def export_vital_file(case: array):
    device_id=case[0]
    case_id=case[1]
    logging.warning("export_vital_file  " + device_id + case_id)

    hook1 = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    # sql_stmt1 = (
    #     "select name from device_{{ ds_nodash }}"
    #     f" where id = '{device_id}'::UUID;"
    # )
    sql_stmt1 = (
        f"select name from device_{EUMC_MIG_DATE}"
        f" where id = '{device_id}'::UUID;"
    )
    logging.warning(sql_stmt1)
    df1 = hook1.get_pandas_df(sql=sql_stmt1)
    name = str(df1.iloc[0,0])
    name= name.replace('/', '-')

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    # sql_stmt = (
    #     "select type, name, format, unit, srate, json_agg(ts order by ts asc) as ts, json_agg(val) as val"
    #     " from track_per_caseid_type_{{ ds_nodash }}"
    #     f" where device_id = '{device_id}'::UUID"
    #     f" and case_id= '{case_id}'"
    #     " group by type, name, format, unit, srate;"
    # )
    sql_stmt = (
        "select type, name, format, unit, srate, json_agg(ts order by ts asc) as ts, json_agg(val) as val"
        f" from track_per_caseid_type_{EUMC_MIG_DATE}"
        f" where device_id = '{device_id}'::UUID"
        f" and case_id= '{case_id}'"
        " group by type, name, format, unit, srate;"
    )
    logging.warning(sql_stmt)
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
    s3_file = ts.strftime('%Y/%m/%d/')
    if is_uploaded(start_str):
        print('already uploaded: ' + start_str)
        return

    track_type_rows_pb = trackTypeRows_pb2.TrackTypeRows()
    for (idx, row_series) in df.iterrows():
        # print('Row Index label : ', idx)
        df_p = pd.DataFrame({'val': row_series['val']})
        if len(df_p.shape) == 0 or len(df_p['val'].shape) == 0 or len(df_p['val'].values.shape) == 0:
            continue
        print(len(df_p.shape), len(df_p['val'].shape), len(df_p['val'].values.shape))
        try:
            df.at[idx , 'val'] =  np.concatenate(df_p['val'].values)
        except:
            continue
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
    pb_filename = f'{start_str}_{name}'
    s3_file += pb_filename + '.tgz'
    print(s3_file)
    f = open(f'{pb_filename}.pb', "wb")
    f.write(track_type_rows_pb.SerializeToString())
    f.close()

    mk_tgz(pb_filename)
    upload_to_minio(f'/tmp/{pb_filename}/{pb_filename}.tgz', s3_file)

def test_normalize():
    arr2: str = "[[60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60], [60]]"
    "[[23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94], [23.94]]"

    json_array2 = json.loads(arr2)
    df_p = pd.DataFrame({'val': json_array2})
    ft = np.concatenate(df_p['val'].values)

@task
def get_postgres_cases(last_sync_date: str):
    # format: yyyymmddhh
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_stmt = (
        "select device_id, case_id"
        f" from caseid_start_to_end_{last_sync_date};"
    )
    # logging.warning(sql_stmt)
    df = hook.get_pandas_df(sql=sql_stmt)
    # [ n rows x 2 columns]
    arr: np.ndarray = df.to_numpy()
    cases =  list(arr)
    # 'device_id: ' + str(case[0]) + ' case_id: ' + str(case[1])
    return cases

def manual_pb_gen():
    cases = ['614d1c80-c10a-11ec-b55c-77808ef42464', '000c062c8d'] #get_postgres_cases(EUMC_MIG_DATE)
    print('total: ', cases)
    export_vital_file(cases)


if __name__ == "__main__":
    print('start test')
    # export_vital_file('130bc450-406b-11ed-8f01-cfef1303339c', '007832df11')
    # test_normalize()
    # print(is_uploaded('20220425_205719'))
    manual_pb_gen()
    print('finish main')

with DAG(
    dag_id="cdss_etl",
    start_date=days_ago(1),
    schedule_interval=timedelta(hours=24),
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

        create_etl_meta >> duplicate_device >> duplicate_ts_kv_dictionary

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
                                    from ts_kv_dictionary_{{ ds_nodash }}
                                    where key = 'caseid')
                            and ts between (select min(ts) as start_ts from raw_track_{{ ds_nodash}}) and (select max(ts) as end_ts from raw_track_{{ ds_nodash }})
                        group by entity_id, case_id;''',
            dag=dag,
        )

        generate_track_per_type = PostgresOperator(
            task_id='generate_track_per_type',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists track_per_type_{{ ds_nodash }};
                    select device_id,
                    json_array_elements(track_data)->>'type' as type,
                    json_array_elements(track_data)->>'name' as name,
                    json_array_elements(track_data)->>'format' as format,
                    json_array_elements(track_data)->>'unit' as unit,
                    json_array_elements(track_data)->>'srate' as srate,
                    floor((json_array_elements(json_array_elements(track_data)->'data')->>'ts')::float * 1000)::bigint as ts,
                    json_array_elements(json_array_elements(track_data)->'data')->'val' as val
                into track_per_type_{{ ds_nodash }}
                from raw_track_{{ ds_nodash }};''',
            dag=dag,
        )

        generate_track_per_caseid_type = PostgresOperator(
            task_id='generate_track_per_caseid_type',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''drop table if exists track_per_caseid_type_{{ ds_nodash }};
                select caseid.case_id, caseid.start_ts, caseid.end_ts, track.*
                into track_per_caseid_type_{{ ds_nodash }}
                from caseid_start_to_end_{{ ds_nodash }} as caseid
                    join track_per_type_{{ ds_nodash }} as track
                    on caseid.device_id = track.device_id
                    and track.ts between caseid.start_ts and caseid.end_ts;
                create index track_per_caseid_type_{{ ds_nodash }}_export
                    on track_per_caseid_type_{{ ds_nodash }}
                    (device_id, case_id, type, name, format, unit, srate, ts);
                ''',
            dag=dag,
        )


        generate_device_case = PostgresOperator(
            task_id='generate_device_case',
            postgres_conn_id=POSTGRES_CONN_ID,
            # sql='''drop table if exists device_case_{{ ds_nodash }};
            #         select device_id, case_id, min(ts) as start_ts, max(ts) as end_ts
            #         into device_case_{{ ds_nodash }}
            #         from track_per_caseid_type_{{ ds_nodash }}
            #         group by device_id, case_id;''',
            sql=f"drop table if exists device_case_{EUMC_MIG_DATE};"
                " select device_id, case_id, min(ts) as start_ts, max(ts) as end_ts"
                f" into device_case_{EUMC_MIG_DATE}"
                f" from track_per_caseid_type_{EUMC_MIG_DATE}"
                " group by device_id, case_id;",
            dag=dag,
        )

        generate_raw_track >> generate_caseid_start_to_end >> generate_track_per_type >> generate_track_per_caseid_type

    with TaskGroup("upload_vital_file", tooltip="Tasks for update upload_vital_file") as upload_vital_file:
        start_export = DummyOperator(task_id="start_export")

        export_vital = PythonOperator.partial(
            task_id='export_vital',
            python_callable=export_vital_file,
            dag=dag,
        ).expand(
            op_args=get_postgres_cases()
        )

        start_export >> export_vital

    with TaskGroup("refresh_meta", tooltip="Tasks for update ETL_META") as refresh_meta:
        update_meta = PostgresOperator(
            task_id='update_meta',
            postgres_conn_id=POSTGRES_CONN_ID,
            # sql='''UPDATE etl_meta em
            #         set last_case_id = d.case_id,
            #         last_start_ts = d.start_ts,
            #         last_end_ts = d.end_ts
            #     FROM device_case_{{ ds_nodash }} as d
            #     WHERE em.device_id = d.device_id;''',
            sql="UPDATE etl_meta em"
                " set last_case_id = d.case_id,"
                " last_start_ts = d.start_ts,"
                " last_end_ts = d.end_ts"
                f" FROM device_case_{EUMC_MIG_DATE} as d"
                " WHERE em.device_id = d.device_id;",
            dag=dag,
        )

        insert_meta = PostgresOperator(
            task_id='insert_meta',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='''INSERT INTO  etl_meta(device_id, last_case_id, last_start_ts, last_end_ts, device_name)
                    select device_id, case_id as last_case_id, start_ts as last_start_ts, end_ts as last_end_ts, d.name as device_name
                    from (select device_id, case_id, start_ts, end_ts, rank() OVER (PARTITION BY device_id ORDER BY start_ts DESC) as rk
                        from device_case_{{ ds_nodash }}
                        where device_id not in (select device_id from etl_meta)
                        group by device_id, case_id, start_ts, end_ts
                        order by device_id, start_ts desc) as dt
                        join device_{{ ds_nodash }} as d on dt.device_id = d.id
                    where rk = 1;''',
            dag=dag,
        )

        update_meta >> insert_meta

    end = DummyOperator(task_id="end")

    start >> process_etl >> duplicate_tb  >> upload_vital_file >> refresh_meta >> end
