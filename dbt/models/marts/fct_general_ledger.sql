/*
    Mart: General Ledger
    ====================
    Join journal entries with the account mapping to produce a fully
    attributed general ledger — one row per journal line with account
    metadata attached.

    This is the single source of truth for all downstream financial
    reports (trial balance, P&L, balance sheet).
*/

with journal as (
    select * from {{ ref('int_journal_entries') }}
),

accounts as (
    select * from {{ ref('stg_account_mapping') }}
),

ledger as (
    select
        j.journal_entry_id,
        j.order_id,
        j.ride_id,
        j.scooter_id,
        j.ride_date,
        j.start_time,
        j.city,
        j.country,
        j.currency,

        j.line_type,
        j.line_number,
        j.entry_side,
        j.line_amount,
        j.coupon_code,

        -- Account metadata from mapping
        a.account_code,
        a.account_name,
        a.account_category,
        a.normal_side,

        -- Signed amounts for easy aggregation:
        --   debit  → positive
        --   credit → negative
        case
            when j.entry_side = 'debit' then  j.line_amount
            when j.entry_side = 'credit' then -j.line_amount
        end as signed_amount

    from journal j
    left join accounts a
        on  j.line_type = a.line_type
        and j.country   = a.country
)

select * from ledger
