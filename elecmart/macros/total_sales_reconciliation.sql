{% test total_sales_matches_transactions(model) %}

select *
from {{ model }}
where total_completed_sales != (
    select coalesce(sum(transaction_total), 0)
    from {{ ref('silver_fact_transaction') }}
    where transaction_status = 'Completed'
)

{% endtest %}