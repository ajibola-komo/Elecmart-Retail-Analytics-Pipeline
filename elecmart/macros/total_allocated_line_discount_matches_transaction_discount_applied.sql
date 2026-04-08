{% test total_allocated_line_discount_matches_transaction_discount_applied(model) %}

with aggregated as (
    select 
        transaction_id,
        sum(allocated_line_discount) as total_allocated_line_discount,
        sum(net_line_revenue) as total_transaction_revenue
    from {{ model }}
    group by transaction_id
)

select 
    a.transaction_id,
    a.total_allocated_line_discount,
    ft.transaction_discount_applied,
    a.total_transaction_revenue,
    ft.transaction_total

from aggregated a
join {{ ref('silver_fact_transaction') }} ft 
    on a.transaction_id = ft.transaction_id

where 
    abs(a.total_allocated_line_discount - ft.transaction_discount_applied) > 0.02
   or abs(a.total_transaction_revenue - ft.transaction_total) > 0.02

{% endtest %}