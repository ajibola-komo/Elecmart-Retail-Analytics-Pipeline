{% test gross_profit_reconciliation(model) %}
select *
from {{ model }}
where gross_profit_for_completed_transactions != (
    select round(coalesce(sum(transaction_total), 0) - coalesce(sum(transaction_cost), 0), 2)
    from {{ ref('silver_fact_transaction') }} where transaction_status = 'Completed'
)

{% endtest %}