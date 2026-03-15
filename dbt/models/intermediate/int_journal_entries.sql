/*
    Intermediate: Journal Entries
    =============================
    Explode each ride into 4 double-entry journal lines:

      Line 1  DR  Accounts Receivable   = sum_with_vat − coupon_amount
      Line 2  DR  Coupon Expense         = coupon_amount  (0 when no coupon)
      Line 3  CR  Ride Revenue           = amount         (net, pre-VAT)
      Line 4  CR  VAT Payable            = vat_amount

    Invariant:  total debits = total credits  per order_id
      (sum_with_vat − coupon) + coupon  =  amount + vat_amount  =  sum_with_vat
*/

with rides as (
    select * from {{ ref('stg_rides') }}
),

-- Generate 4 lines per ride using a cross join with a line-type spine
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
        r.coupon_code

    from rides r
    cross join line_types lt
)

select
    md5(cast(order_id as varchar) || '|' || cast(line_type as varchar)) as journal_entry_id,
    *
from journal_lines
