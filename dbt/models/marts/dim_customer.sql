{{
    config(
        materialized = 'table',
        file_format = 'delta'
    )
}}

with snap_address as 
(
    select 
        AddressID,
        AddressLine1,
        City,
        StateProvince,
        CountryRegion,
        PostalCode
    from {{ ref('address_snapshot') }}
    where dbt_valid_to is null
),
snap_customeraddress AS
(
    select CustomerID
      ,AddressID
      ,AddressType
    from {{ ref('customeraddress_snapshot') }}
    where dbt_valid_to is null
),
snap_customer as 
(
    select CustomerID
      , concat(COALESCE(FirstName,' '),' ',COALESCE(MiddleName,' '),' ',COALESCE(LastName,' ')) as FullName
    from {{ ref('customer_snapshot') }}
    where dbt_valid_to is null
),
transformed as 
(
    SELECT
    ROW_NUMBER() over(order by c.CustomerID) as customer_sk
    , c.CustomerID
    , C.FullName
    , ca.AddressID
    , ca.AddressType
    , a.AddressLine1
    , a.City
    , a.StateProvince
    , a.CountryRegion
    , a.PostalCode
    from snap_customer c 
    inner join snap_customeraddress ca on c.CustomerID = ca.CustomerID
    inner join snap_address a on ca.AddressID = a.AddressID
)
select * from transformed