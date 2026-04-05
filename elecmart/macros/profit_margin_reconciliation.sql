{% test profit_margin_reconciliation(model) %}
select *
from {{ model }}
where profit_margin_for_completed_transactions != (
    select 
round(
    (coalesce(sum(transaction_total),0) - coalesce(sum(transaction_cost), 0))
/ nullif(coalesce(sum(transaction_total), 0), 0) * 100, 2)
    from {{ ref('silver_fact_transaction') }} where transaction_status = 'Completed'
)
{% endtest %}