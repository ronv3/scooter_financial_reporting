select * from data_lake.rides where order_id in ('4c4be777-6142-81f0-2fc1-5fa383e083cc','3100874b-61bf-6b94-2a6c-930226969acb');

select * from data_warehouse.stg_rides where order_id in ('4c4be777-6142-81f0-2fc1-5fa383e083cc','3100874b-61bf-6b94-2a6c-930226969acb');;

select * from data_warehouse.stg_account_mapping where country = 'Estonia';

select * from data_warehouse.int_journal_entries where order_id in ('4c4be777-6142-81f0-2fc1-5fa383e083cc','3100874b-61bf-6b94-2a6c-930226969acb') order by scooter_id;

select * from data_warehouse.stg_chart_of_accounts;

select * from data_warehouse.fct_general_ledger where order_id in ('4c4be777-6142-81f0-2fc1-5fa383e083cc','3100874b-61bf-6b94-2a6c-930226969acb') order by scooter_id;

select * from data_warehouse.fct_trial_balance where date(reporting_period) = date('2026-02-01') and country = 'Estonia';

select * from data_warehouse.rpt_balance_sheet_detail;
select * from data_warehouse.rpt_balance_sheet_summary;


select * from data_warehouse.rpt_income_statement_detail;
select * from data_warehouse.rpt_income_statement_summary;