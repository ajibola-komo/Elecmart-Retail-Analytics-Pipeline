{{
    config(
        materialized='view',
        description="This model calculates the total revenue and total cost for completed transactions, which will be used for KPI reconciliation tests in the gold_kpi_overview_completed_transactions model."
    )
}}
select coalesce(sum(transaction_total), 0) as total_sales,
coalesce(sum(transaction_cost), 0) as total_cost from {{ref('silver_fact_transaction')}} where transaction_status = 'Completed'
