/*
    Mart: Trial Balance
    ===================
    Aggregate the general ledger into account-level totals,
    broken down by reporting_period (month).

    Materialized as INCREMENTAL with delete+insert by reporting_period.
    On each run, the pre-hook deletes the current period's trial balance
    rows, then the SELECT re-aggregates only that period's GL data.
    Previous periods' aggregated rows remain untouched — this acts as
    a soft period close, keeping historical trial balances stable.

    On first run (or --full-refresh) the entire GL is aggregated.

    The trial balance must satisfy per period:
      total debits = total credits

    Downstream reports filter this table:
      - Income statement: current period only
      - Balance sheet:    cumulative through current period end
*/

{{
    config(
        materialized='incremental',
        unique_key=['reporting_period', 'account_code', 'country'],
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('reporting_period') }}"
        ] if is_incremental() else []
    )
}}

with ledger as (
    select *
    from {{ ref('fct_general_ledger') }}
    {% if is_incremental() %}
    where reporting_period >= date_trunc('month', {{ get_start_date() }})
      and reporting_period <= date_trunc('month', {{ get_end_date() }})
    {% endif %}
),

aggregated as (
    select
        reporting_period,
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
        reporting_period,
        account_code,
        account_name,
        account_category,
        normal_side,
        country
)

select * from aggregated
order by reporting_period, account_code, country
