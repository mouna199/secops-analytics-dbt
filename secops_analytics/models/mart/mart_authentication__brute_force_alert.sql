with failed_attempts as (
     select * from {{ref('int_authentication__failed_attempts') }}   
), 
mart as 
(
select 
    user_id,
    source_ip,
    status,
    event_timestamp,
    failed_attempts_by_user_1h,
    failed_attempts_by_user_24h,
    -- seuils simples
    case
        when failed_attempts_by_user_1h >= 10  then 'critical'
        when failed_attempts_by_user_1h >= 5   then 'medium'
        when failed_attempts_by_user_1h >= 3   then 'low'
        else null
    end as alert_severity

from failed_attempts
)
select * from mart
where alert_severity is not null