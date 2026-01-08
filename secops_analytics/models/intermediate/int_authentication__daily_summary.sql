with auth_logs as(
    select * from {{ref('stg_raw_secops__authentication_logs')}}
),
daily_summary as (
    select 
        user_id,
        DATE(event_timestamp) as event_date,
        count(*) as total_attempts
    from auth_logs
    group by 1, 2
)
select * from daily_summary