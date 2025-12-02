{% snapshot customer_snapshot %}

{{ 
    config(
        file_format = 'delta',
        target_schema = 'snapshots',
        invalidates_hard_deletes = True,
        unique_key = 'CustomerID',
        strategy = 'check',
        check_cols = 'all'
    )
}}

with source_customer as(
    select CustomerID
      ,NameStyle
      ,Title
      ,FirstName
      ,MiddleName
      ,LastName
      ,Suffix
      ,CompanyName
      ,SalesPerson
      ,EmailAddress
      ,Phone
    from {{source('saleslt','customer')}}
)
select * from source_customer

{% endsnapshot %}