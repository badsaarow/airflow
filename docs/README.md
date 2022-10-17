# Airflow

## Step

```sql
select type, name, format, json_agg(ts order by ts asc) as ts, json_agg(val) as val
from track_per_caseid_type
where device_id = '130bc450-406b-11ed-8f01-cfef1303339c'
and case_id = '007832df11'
group by type, name, format, unit, srate;
```
