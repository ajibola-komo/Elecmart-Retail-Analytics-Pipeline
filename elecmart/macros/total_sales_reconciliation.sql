{% test total_sales_matches_transactions(model) %}

select *
from {{ model }}
where total_revenue != (
    select coalesce(sum(transaction_total), 0)
    from {{ ref('gold_fact_completed_transaction') }}
)

{% endtest %}