/*
    Mart: Trial Balance
    ===================
    Aggregate the general ledger into account-level totals.
    The trial balance must satisfy:  total debits = total credits.

    This model provides a quick validation checkpoint and feeds
    both the P&L and the Balance Sheet.
*/

with ledger as (
    select * from {{ ref('fct_general_ledger') }}
),

aggregated as (
    select
        account_code,
        account_name,
        account_category,
        normal_side,
        country,

        sum(case when entry_side = 'debit'  then line_amount else 0 end) as total_debit,
        sum(case when entry_side = 'credit' then line_amount else 0 end) as total_credit,
        sum(signed_amount) as net_balance,

        count(*) as entry_count

    from ledger
    group by
        account_code,
        account_name,
        account_category,
        normal_side,
        country
)

select * from aggregated
order by account_code, country
