SELECT
    {{dbt_utils.generate_surrogate_key(['season_id','season_year'])}} AS season_key,
    season_id,
    season_year,
    season_start_date,
    season_end_date,
    season_current_matchday
FROM {{ ref('int_seasons') }}