-- payment type 번호를 받아 해당하는 결제 이름을 반환하는 매크로
{% macro get_payment_type_description(payment_type) -%}

    case {{ payment_type }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'Empty'
    end

{%- endmacro %}