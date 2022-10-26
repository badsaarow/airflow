# Airflow

## Connections
```
airflow db reset
airflow db init
airflow connections list
```

```
airflow connections import connections-tb.yaml
airflow tasks test cdss_etl duplicate_tb.duplicate_device

```

## airflow info

```
Apache Airflow
version                | 2.4.1
executor               | SequentialExecutor
task_logging_handler   | airflow.utils.log.file_task_handler.FileTaskHandler
sql_alchemy_conn       | sqlite:////Users/sungyong/workspace/airflow/airflow.db
dags_folder            | /Users/sungyong/workspace/airflow/dags
plugins_folder         | /Users/sungyong/workspace/airflow/plugins
base_log_folder        | /Users/sungyong/workspace/airflow/logs
remote_base_log_folder |


System info
OS              | Mac OS
architecture    | arm
uname           | uname_result(system='Darwin', node='m1pro.local', release='21.6.0', version='Darwin Kernel Version 21.6.0:
                | Mon Aug 22 20:19:52 PDT 2022; root:xnu-8020.140.49~2/RELEASE_ARM64_T6000', machine='arm64')
locale          | ('en_US', 'UTF-8')
python_version  | 3.9.6 (default, Aug  5 2022, 15:21:02)  [Clang 14.0.0 (clang-1400.0.29.102)]
python_location | /Library/Developer/CommandLineTools/usr/bin/python3


Tools info
git             | git version 2.38.1
ssh             | OpenSSH_8.6p1, LibreSSL 3.3.6
kubectl         | NOT AVAILABLE
gcloud          | Google Cloud SDK 406.0.0
cloud_sql_proxy | NOT AVAILABLE
mysql           | mysql  Ver 8.0.31 for macos12.6 on arm64 (Homebrew)
sqlite3         | 3.37.0 2021-12-09 01:34:53 9ff244ce0739f8ee52a3e9671adb4ee54c83c640b02e3f9d185fd2f9a179aapl
psql            | psql (PostgreSQL) 14.5 (Homebrew)


Paths info
airflow_home    | /Users/sungyong/workspace/airflow
system_path     | /Users/sungyong/.local/share/fig/plugins/git-open:/Users/sungyong/.bun/bin:/opt/homebrew/opt/mysql-client/
                | bin:/Users/sungyong/.ops/bin:/opt/homebrew/bin:/opt/homebrew/sbin:/Users/sungyong/.nvm/versions/node/v16.1
                | 7.0/bin:/Users/sungyong/bin:/Users/sungyong/.local/bin:/Users/sungyong/go/bin:/Users/sungyong/.krew/bin:/o
                | pt/homebrew/bin:/opt/homebrew/sbin:/Users/sungyong/bin:/Users/sungyong/.local/bin:/Users/sungyong/go/bin:/
                | usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/share/dotnet:/opt/X11/bin:~/.dotn
                | et/tools:/Library/Apple/usr/bin:/Library/Frameworks/Mono.framework/Versions/Current/Commands:/Users/sungyo
                | ng/.cargo/bin:/Users/sungyong/.fig/bin:/Users/sungyong/.local/bin:/Users/sungyong/Library/Android/sdk/tool
                | s:/Users/sungyong/Library/Android/sdk/platform-tools:/Applications/Visual Studio
                | Code.app/Contents/Resources/app/bin:/opt/homebrew/opt/fzf/bin:/Users/sungyong/.local/share/fig/plugins/git
                | -extra-commands_unixorn/bin:/Users/sungyong/.local/share/fig/plugins/git-extra-commands_unixorn/bin
python_path     | /usr/local/bin:/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/lib/p
                | ython39.zip:/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/lib/pyth
                | on3.9:/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/lib/python3.9/
                | lib-dynload:/Users/sungyong/Library/Python/3.9/lib/python/site-packages:/Library/Developer/CommandLineTool
                | s/Library/Frameworks/Python3.framework/Versions/3.9/lib/python3.9/site-packages:/Library/Python/3.9/site-p
                | ackages:/Users/sungyong/workspace/airflow/dags:/Users/sungyong/workspace/airflow/config:/Users/sungyong/wo
                | rkspace/airflow/plugins
airflow_on_path | True


Providers info
apache-airflow-providers-celery     | 3.0.0
apache-airflow-providers-common-sql | 1.2.0
apache-airflow-providers-ftp        | 3.1.0
apache-airflow-providers-http       | 4.0.0
apache-airflow-providers-imap       | 3.0.0
apache-airflow-providers-postgres   | 5.2.2
apache-airflow-providers-sqlite     | 3.2.1
```

```sql
create table etl_meta
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

create index etl_meta_last_start_ts_index
    on etl_meta (last_start_ts desc);

create index etl_meta_last_end_ts_index
    on etl_meta (last_end_ts desc);

create index track_per_caseid_type_20221023_export
on track_per_caseid_type_20221023 (device_id, case_id, type, name, format, unit, srate, ts);

```
