{% macro mask_email(email) %}
  CONCAT(SUBSTR(email, 0, 2), '*****@*****.com')
{% endmacro %}

{% macro mask_phone(phone) %}
  CONCAT(SUBSTR(CAST(phone AS STRING), 0, 3), 'XXXXXXX')
{% endmacro %}

{% macro mask_aadhaar(aadhaar) %}
  'XXXXXXXXXXXX'
{% endmacro %}

{% macro mask_dob(dob) %}
  'XXXX-XX-XX'
{% endmacro %}
