SELECT
    {{dbt_utils.generate_surrogate_key(['team_id'])}} AS team_key,
    team_id,
    team_name,
    team_short_name,
    team_tla,
    team_crest,
    team_club_colors,
    team_founded,
    team_address,
    team_website,
    team_venue,
    area_id,
    area_name,
    area_code,
    area_flag
FROM {{ ref('int_teams') }}