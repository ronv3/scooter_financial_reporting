/*
    Report: Balance Sheet
    =====================
    Assets = Liabilities + Equity

    In our simplified model:
      - Assets     = Accounts Receivable (what customers owe / have paid)
      - Liabilities = VAT Payable (tax owed to government)
      - Equity     = Retained Earnings = Revenue − Expenses (net income)

    The balance sheet equation MUST hold:
      Assets = Liabilities + Equity
*/

with trial_balance as (
    select * from {{ ref('fct_trial_balance') }}
),

-- Assets: accounts with category = 'asset', normal_side = debit
assets as (
    select
        account_code,
        account_name,
        country,
        net_balance as balance   -- positive for debit-normal accounts
    from trial_balance
    where account_category = 'asset'
),

-- Liabilities: accounts with category = 'liability', normal_side = credit
liabilities as (
    select
        account_code,
        account_name,
        country,
        -net_balance as balance  -- flip sign: credit-normal → positive
    from trial_balance
    where account_category = 'liability'
),

-- Equity = Retained Earnings = Revenue - Expenses
-- Revenue: credit-normal (negative signed_amount) → flip to positive
-- Expenses: debit-normal (positive signed_amount) → subtract
retained_earnings as (
    select
        sum(case
            when account_category = 'revenue' then -net_balance   -- credit → positive
            when account_category = 'expense' then -net_balance   -- debit → negative (subtract)
            else 0
        end) as balance
    from trial_balance
    where account_category in ('revenue', 'expense')
),

-- Assemble the balance sheet
balance_sheet as (
    select 1 as sort_order, 'Assets' as section,
           account_code, account_name, country, balance
    from assets

    union all

    select 2 as sort_order, 'Liabilities' as section,
           account_code, account_name, country, balance
    from liabilities

    union all

    select 3 as sort_order, 'Equity' as section,
           '3000' as account_code,
           'Retained Earnings' as account_name,
           'ALL' as country,
           balance
    from retained_earnings
),

-- Validation: Assets = Liabilities + Equity
totals as (
    select
        sum(case when section = 'Assets'      then balance else 0 end) as total_assets,
        sum(case when section = 'Liabilities'  then balance else 0 end) as total_liabilities,
        sum(case when section = 'Equity'       then balance else 0 end) as total_equity
    from balance_sheet
)

select
    bs.*,
    t.total_assets,
    t.total_liabilities,
    t.total_equity,
    t.total_liabilities + t.total_equity as liabilities_plus_equity,
    abs(t.total_assets - (t.total_liabilities + t.total_equity)) < 0.01 as equation_balanced
from balance_sheet bs
cross join totals t
order by sort_order, account_code, country
