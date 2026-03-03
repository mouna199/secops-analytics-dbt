with source as (
     select * from {{ref('stg_raw_secops__authentication_logs') }}   
), 
renamed as 
(
select 
user_id,
source_ip,
status,
event_timestamp, 
countif(status = 'failure') over (partition by user_id
                                  order by unix_seconds(event_timestamp)
                                  range between 3600 preceding and CURRENT row) as failed_attempts_by_user_1h,
countif(status = 'failure') over (partition by user_id
                                  order by unix_seconds(event_timestamp)
                                  range between 86400 preceding and CURRENT row) as failed_attempts_by_user_24h
from source
)
select * from renamed