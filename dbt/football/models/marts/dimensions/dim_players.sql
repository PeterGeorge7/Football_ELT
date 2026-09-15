SELECT
    {{dbt_utils.generate_surrogate_key(['player_id'])}} as player_key,
    player_id,
    player_name,
    player_dateOfBirth,
    player_nationality
FROM {{ ref('int_players') }} as p