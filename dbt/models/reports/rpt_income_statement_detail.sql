/*
    Report: Income Statement — Detail
    ==================================
    Account-level revenue and expense line items per reporting period.
    No totals — see rpt_income_statement_summary for aggregates.

    Materialized as INCREMENTAL with delete+insert by period_end.
    Each run produces the current period's rows; previous periods'
    rows remain untouched, building up a historical series.
    On first run (or --full-refresh) only the current period is produced.

    Sign convention:
      Revenue:  net_balance is negative (credit-normal) → flip to positive
      Expenses: net_balance is positive (debit-normal)  → show as-is
*/

{{
    config(
        materialized='incremental',
        unique_key=['period_end', 'section', 'account_code', 'country'],
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('period_end') }}"
        ] if is_incremental() else []
    )
}}

with trial_balance as (
    select *
    from {{ ref('fct_trial_balance') }}
    where reporting_period >= date_trunc('month', {{ get_start_date() }})
      and reporting_period <= date_trunc('month', {{ get_end_date() }})
),

-- Revenue: credit-normal accounts → net_balance is negative → flip sign
revenue as (
    select
        account_code,
        account_name,
        country,
        -sum(net_balance) as amount
    from trial_balance
    where account_category = 'revenue'
    group by account_code, account_name, country
),

-- Expenses: debit-normal accounts → net_balance is positive → show as-is
expenses as (
    select
        account_code,
        account_name,
        country,
        sum(net_balance) as amount
    from trial_balance
    where account_category = 'expense'
    group by account_code, account_name, country
)

select
    {{ get_start_date() }} as period_start,
    {{ get_end_date() }}   as period_end,
    1 as sort_order,
    'Revenue' as section,
    account_code,
    account_name,
    country,
    amount
from revenue

union all

select
    {{ get_start_date() }} as period_start,
    {{ get_end_date() }}   as period_end,
    2 as sort_order,
    'Expenses' as section,
    account_code,
    account_name,
    country,
    amount
from expenses

order by sort_order, account_code, country
