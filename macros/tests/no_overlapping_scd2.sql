{% test no_overlapping_scd2(model, key_column, valid_from_col, valid_to_col) %}

with scd as (
    select
        {{ key_column }} as key_value,
        {{ valid_from_col }} as valid_from,
        {{ valid_to_col }} as valid_to
    from {{ model }}
),

pairs as (
    select
        a.key_value,
        a.valid_from,
        a.valid_to,
        b.valid_from as other_from,
        b.valid_to as other_to
    from scd a
    join scd b
        on a.key_value = b.key_value
        and a.valid_from < b.valid_to
        and b.valid_from < a.valid_to
)

select * from pairs

{% endtest %}
