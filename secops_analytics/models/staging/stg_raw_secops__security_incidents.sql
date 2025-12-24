with source as (
     select * from {{source('raw_secops', 'security_incidents') }}   
), 
renamed as (
    select 
        incident_id,
        created_at,
        acknowledged_at,
        resolved_at,
        status,
        priority,
        category,
        false_positive,
        sla_breached
    from source 
)
select * from renamed