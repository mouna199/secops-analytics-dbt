with source as (
    select * from {{source('raw_secops','network_traffic_logs')}}
),
renamed as (
    select 
        event_id,
        event_timestamp,
        source_ip,
        destination_ip,
        destination_port,
        protocol,
        action,
        bytes_sent,
        threat_detected,
        threat_type,
        threat_severity
    from source
)
select * from renamed