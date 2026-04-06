{% test gross_profit_reconciliation(model) %}
select *
from {{ model }}
where gross_profit != (
    select round(coalesce(sum(transaction_total), 0) - coalesce(sum(transaction_cost), 0), 2)
    from {{ ref('gold_fact_completed_transaction') }}
)

{% endtest %}