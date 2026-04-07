/*
    Report: Balance Sheet — Summary
    ================================
    One row per reporting date with total assets, total liabilities,
    total equity, and the accounting equation validation.

    Materialized as INCREMENTAL with delete+insert by report_date.
    Each run re-aggregates only the current period's detail snapshot;
    historical summary rows remain untouched.

    The balance sheet equation MUST hold:
      Assets = Liabilities + Equity

    The threshold for equation_balanced uses 0.001 (one mill) rather
    than exact zero.  All underlying amounts are decimal(12,2) and
    the pipeline uses only addition/subtraction (no division), so in
    practice the imbalance should be exactly 0.  The 0.001 margin is
    a defensive guard against hypothetical floating-point artefacts
    while remaining well below the smallest reportable unit (0.01).
*/

{{
    config(
        materialized='incremental',
        unique_key=['report_date'],
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('report_date') }}"
        ] if is_incremental() else []
    )
}}

with detail as (
    select * from {{ ref('rpt_balance_sheet_detail') }}
    {% if is_incremental() %}
    where report_date >= {{ get_start_date() }}
      and report_date <= {{ get_end_date() }}
    {% endif %}
)

select
    report_date,
    sum(case when section = 'Assets'      then balance else 0 end) as total_assets,
    sum(case when section = 'Liabilities'  then balance else 0 end) as total_liabilities,
    sum(case when section = 'Equity'       then balance else 0 end) as total_equity,
    sum(case when section = 'Liabilities'  then balance else 0 end)
      + sum(case when section = 'Equity'   then balance else 0 end) as liabilities_plus_equity,
    abs(
        sum(case when section = 'Assets' then balance else 0 end)
        - (sum(case when section = 'Liabilities' then balance else 0 end)
           + sum(case when section = 'Equity' then balance else 0 end))
    ) < 0.001 as equation_balanced
from detail
group by report_date
