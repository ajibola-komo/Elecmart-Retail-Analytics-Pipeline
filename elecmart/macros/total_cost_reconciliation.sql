{% test total_cost_matches_transactions(model) %}

select *
from {{ model }}
where total_cogs != (
    select coalesce(sum(transaction_cost), 0) from {{ ref('gold_fact_completed_transaction') }}
)

{% endtest %}