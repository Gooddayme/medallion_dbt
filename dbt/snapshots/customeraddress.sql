{% snapshot customeraddress_snapshot %}
{{ config(
    file_format = 'delta',
    target_schema = 'snapshots',
    invalidates_hard_deletes = True,
    unique_key = "CustomerId||'-'||AddressId",
    strategy = 'check',
    check_cols = ['AddressType']
)}}

with source_customeraddress as (
    select CustomerID
      ,AddressID
      ,AddressType
    from {{source('saleslt','customeraddress')}}
)
select * from source_customeraddress

{% endsnapshot %}