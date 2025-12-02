--address_snapshot scd
{% snapshot address_snapshot%}

{{
    config(
        file_format = "delta",
        target_schema = "snapshots",
        invalidates_hard_deletes = True,
        unique_key = 'AddressID',
        strategy = 'check',
        check_cols = ['AddressLine1']
    )
}}
with bronze_address as (
    select
        AddressID,
        AddressLine1,
        AddressLine2,
        City,
        StateProvince,
        CountryRegion,
        PostalCode
    from {{ source('saleslt', 'address') }}
)
select * from bronze_address

{% endsnapshot %}