{% macro get_payment_description(payment_type)%}
    case {{payment_type}}
        when 1 then 'Credit Card'
        when 2 then 'Cash'
        when 3 then 'No Charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided Trips'
    End

{% endmacro%}