{% test total_cost_matches_transactions(model) %}

select *
from {{ model }}
where total_cost_for_completed_transactions != (
    select total_completed_cost from {{ ref('get_rev_and_cost') }}
)

{% endtest %}