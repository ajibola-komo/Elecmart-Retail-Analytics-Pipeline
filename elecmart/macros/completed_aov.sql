{% test completed_aov(model) %}

select *
from {{ model }}
where aov != (
    select round(sum(transaction_total) / nullif(count(transaction_id), 0), 2)
    from {{ ref('gold_fact_completed_transaction') }} 
)

{% endtest %}