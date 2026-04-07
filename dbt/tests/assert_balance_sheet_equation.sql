/*
    Singular test: Balance sheet equation
    ======================================
    Assets = Liabilities + Equity must hold.

    Now references rpt_balance_sheet_summary which contains one row
    per report_date with the equation_balanced flag (threshold 0.001).
*/

select
    report_date,
    total_assets,
    total_liabilities,
    total_equity,
    liabilities_plus_equity
from {{ ref('rpt_balance_sheet_summary') }}
where equation_balanced = false
