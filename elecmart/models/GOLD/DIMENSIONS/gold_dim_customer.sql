select 
c.customer_id, 
c.email_address, 
c.first_name, 
c.last_name, 
c.gender, 
c.customer_persona, 
c.age_group,
l.country, 
l.state_province, 
l.city, 
l.location_type, 
c.signup_date, 
c.signup_date_id, 
c.signup_channel, 
c.loyalty_status,
c.income_bracket, 
c.email_opt_in, 
c.sms_opt_in
 from {{ref("silver_dim_customer")}} c inner join {{ref("silver_dim_location")}} l
    on c.location_id = l.location_id