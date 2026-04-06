{% test total_completed_transactions(model) %}

select *
from {{ model }}
where total_transactions != (
    select coalesce(count(transaction_id), 0)
    from {{ ref('gold_fact_completed_transaction') }}
)

{% endtest %}