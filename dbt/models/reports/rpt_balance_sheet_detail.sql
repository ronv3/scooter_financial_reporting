/*
    Report: Balance Sheet — Detail
    ===============================
    Account-level cumulative balances as of the period end date.
    No totals — see rpt_balance_sheet_summary for the equation check.

    Materialized as INCREMENTAL with delete+insert by report_date.
    Each run produces a cumulative snapshot for the current period's
    end date; previous periods' snapshots remain untouched, building
    a historical series of balance sheets.

    In our simplified model:
      - Assets      = Accounts Receivable (what customers owe / have paid)
      - Liabilities = VAT Payable (tax owed to government)
      - Equity      = Retained Earnings = cumulative Revenue − Expenses
*/

{{
    config(
        materialized='incremental',
        unique_key=['report_date', 'section', 'account_code', 'country'],
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('report_date') }}"
        ] if is_incremental() else []
    )
}}

with trial_balance as (
    -- Cumulative: everything up to and including the current period
    select *
    from {{ ref('fct_trial_balance') }}
    where reporting_period <= date_trunc('month', {{ get_end_date() }})
),

-- Sum across all periods to get cumulative balances
cumulative as (
    select
        account_code,
        account_name,
        account_category,
        normal_side,
        country,
        sum(net_balance) as net_balance
    from trial_balance
    group by
        account_code,
        account_name,
        account_category,
        normal_side,
        country
),

-- Assets: accounts with category = 'asset', normal_side = debit
assets as (
    select
        account_code,
        account_name,
        country,
        net_balance as balance   -- positive for debit-normal accounts
    from cumulative
    where account_category = 'asset'
),

-- Liabilities: accounts with category = 'liability', normal_side = credit
liabilities as (
    select
        account_code,
        account_name,
        country,
        -net_balance as balance  -- flip sign: credit-normal → positive
    from cumulative
    where account_category = 'liability'
),

-- Equity = Retained Earnings = cumulative Revenue - Expenses
retained_earnings as (
    select
        sum(case
            when account_category = 'revenue' then -net_balance
            when account_category = 'expense' then -net_balance
            else 0
        end) as balance
    from cumulative
    where account_category in ('revenue', 'expense')
)

select
    {{ get_end_date() }} as report_date,
    1 as sort_order,
    'Assets' as section,
    account_code,
    account_name,
    country,
    balance
from assets

union all

select
    {{ get_end_date() }} as report_date,
    2 as sort_order,
    'Liabilities' as section,
    account_code,
    account_name,
    country,
    balance
from liabilities

union all

select
    {{ get_end_date() }} as report_date,
    3 as sort_order,
    'Equity' as section,
    '3000' as account_code,
    'Retained Earnings' as account_name,
    'ALL' as country,
    balance
from retained_earnings

order by sort_order, account_code, country
