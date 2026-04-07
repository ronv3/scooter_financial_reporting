/*
    Intermediate: Journal Entries
    =============================
    Explode each ride into double-entry journal lines.

    Materialized as an INCREMENTAL TABLE (delete+insert per period)
    so that journal entries are persisted and auditable.  This allows
    downstream consumers and auditors to inspect the exact journal
    lines that were posted for any historical period, even after the
    source seeds have been refreshed.

    Rides without a coupon produce 3 lines; rides with a coupon
    produce 4 lines.  Zero-amount lines are never posted — this
    is standard accounting practice.

      DR  Accounts Receivable   = sum_with_vat − coupon_amount
      DR  Coupon Expense         = coupon_amount  (only when coupon used)
      CR  Ride Revenue           = amount         (net, pre-VAT)
      CR  VAT Payable            = vat_amount

    Invariant:  total debits = total credits  per order_id
      (sum_with_vat − coupon) + coupon  =  amount + vat_amount  =  sum_with_vat
*/

{{
    config(
        materialized='incremental',
        unique_key='journal_entry_id',
        on_schema_change='append_new_columns',
        pre_hook=[
            "{{ delete_period('ride_date') }}"
        ] if is_incremental() else []
    )
}}

with rides as (
    select * from {{ ref('stg_rides') }}
),

-- Spine of possible journal line types per ride
line_types as (
    select 'receivable'     as line_type, 1 as line_number union all
    select 'coupon_expense' as line_type, 2 as line_number union all
    select 'revenue'        as line_type, 3 as line_number union all
    select 'vat_payable'    as line_type, 4 as line_number
),

journal_lines as (
    select
        r.order_id,
        r.ride_id,
        r.scooter_id,
        r.ride_date,
        r.start_time,
        r.city,
        r.country,
        r.currency,

        lt.line_type,
        lt.line_number,

        -- Determine debit/credit side
        case
            when lt.line_type in ('receivable', 'coupon_expense') then 'debit'
            else 'credit'
        end as entry_side,

        -- Calculate amount for each line type
        case lt.line_type
            when 'receivable'     then r.sum_with_vat_amount - r.coupon_amount
            when 'coupon_expense' then r.coupon_amount
            when 'revenue'        then r.amount
            when 'vat_payable'    then r.vat_amount
        end as line_amount,

        -- Carry coupon code for traceability
        r.coupon_code,

        -- Audit metadata
        current_timestamp as loaded_at

    from rides r
    cross join line_types lt
)

select
    md5(cast(order_id as varchar) || '|' || cast(line_type as varchar)) as journal_entry_id,
    *
from journal_lines
where line_amount > 0
