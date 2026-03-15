/*
    Report: Income Statement (Profit & Loss)
    =========================================
    Revenue minus expenses = net income.

    Revenue accounts (4xxx) have credit normal side → shown as positive.
    Expense accounts (6xxx) have debit normal side  → shown as positive cost.
*/

with trial_balance as (
    select * from {{ ref('fct_trial_balance') }}
),

revenue as (
    select
        account_code,
        account_name,
        country,
        total_credit as revenue_amount
    from trial_balance
    where account_category = 'revenue'
),

expenses as (
    select
        account_code,
        account_name,
        country,
        total_debit as expense_amount
    from trial_balance
    where account_category = 'expense'
),

revenue_total as (
    select
        'TOTAL REVENUE' as label,
        sum(revenue_amount) as amount
    from revenue
),

expense_total as (
    select
        'TOTAL EXPENSES' as label,
        sum(expense_amount) as amount
    from expenses
),

-- Detailed line items
line_items as (
    select
        1 as sort_order,
        'Revenue' as section,
        account_code,
        account_name,
        country,
        revenue_amount as amount
    from revenue

    union all

    select
        2 as sort_order,
        'Expenses' as section,
        account_code,
        account_name,
        country,
        expense_amount as amount
    from expenses
)

select
    sort_order,
    section,
    account_code,
    account_name,
    country,
    amount,
    -- Running context: what feeds into net income
    sum(case when section = 'Revenue' then amount else 0 end)
        over () as total_revenue,
    sum(case when section = 'Expenses' then amount else 0 end)
        over () as total_expenses,
    sum(case when section = 'Revenue' then amount else -amount end)
        over () as net_income

from line_items
order by sort_order, account_code, country
