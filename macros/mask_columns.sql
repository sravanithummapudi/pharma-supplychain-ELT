{% macro mask_column(column_name, mask_type='default') %}
    {% if mask_type == 'email' %}
        CONCAT(SAFE.SUBSTR({{ column_name }}, 0, 2), '*****@*****.com')
    {% elif mask_type == 'phone' %}
        SAFE.SUBSTR(CAST({{ column_name }} AS STRING), 0, 3) || 'XXXXXXX'
    {% elif mask_type == 'name' %}
        CONCAT(SAFE.SUBSTR({{ column_name }}, 1, 2), '*******')
    {% else %}
        '***MASKED***'
    {% endif %}
{% endmacro %}
