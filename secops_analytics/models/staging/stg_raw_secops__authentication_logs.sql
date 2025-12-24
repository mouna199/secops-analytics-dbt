with source as (
    select * from {{source('raw_secops','authentication_logs')}}
),
renamed as (
    select 
        event_id,
        event_timestamp,
        user_id,
        email,
        event_type,
        status,
        source_ip,
        source_country,
        device_type,
        mfa_enabled,
        failure_reason
    from source
)
select * from renamed