/*
    Report: Income Statement — Summary
    ====================================
    One row per reporting period with total revenue, total expenses,
    and net income.  Downstream dashboards and the thesis narrative
    reference this model for period-level P&L figures.

    Materialized as INCREMENTAL with delete+insert by period_end.
    Each run re-aggregates only the current period's detail rows;
    historical summary rows remain untouched.
*/

{{
    config(
        materialized='incremental',
        unique_key=['period_end'],
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('period_end') }}"
        ] if is_incremental() else []
    )
}}

with detail as (
    select * from {{ ref('rpt_income_statement_detail') }}
    {% if is_incremental() %}
    where period_end >= {{ get_start_date() }}
      and period_end <= {{ get_end_date() }}
    {% endif %}
)

select
    period_start,
    period_end,
    sum(case when section = 'Revenue'  then amount else 0 end) as total_revenue,
    sum(case when section = 'Expenses' then amount else 0 end) as total_expenses,
    sum(case when section = 'Revenue'  then amount else 0 end)
      - sum(case when section = 'Expenses' then amount else 0 end) as net_income
from detail
group by period_start, period_end
