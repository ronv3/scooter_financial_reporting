/*
    Staging: account_mapping
    Clean types for the chart-of-accounts / journal line-type mapping.
*/

with source as (
    select * from {{ source('data_lake', 'account_mapping') }}
)

select
    line_type,
    country,
    account_code,
    account_name,
    account_category,   -- asset | liability | revenue | expense
    normal_side         -- debit | credit
from source
