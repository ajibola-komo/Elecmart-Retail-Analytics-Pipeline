{% test total_completed_transactions(model) %}

select *
from {{ model }}
where total_completed_transactions != (
    select coalesce(count(transaction_id), 0)
    from {{ ref('silver_fact_transaction') }}  where transaction_status = 'Completed'
)

{% endtest %}