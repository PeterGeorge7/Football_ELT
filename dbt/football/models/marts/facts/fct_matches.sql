SELECT
    {{dbt_utils.generate_surrogate_key(['match_id'])}} as match_key,
    m.match_id,
    dim_coms.competition_key,
    dim_seasons.season_key,
    dim_date.date_key,
    m.match_datetime,
    dim_teams_home.team_key as home_team_key,
    dim_teams_away.team_key as away_team_key,
    m.home_team_score,
    m.away_team_score,
    {# m.winner_team_id, #}
    dim_teams_winner.team_key as winner_team_key,
    m.match_result,
    m.match_status,
    m.duration,
    m.match_matchday,
    m.match_stage,
    m.match_group

FROM {{ ref('int_matches') }} AS m

LEFT JOIN {{ ref('dim_date') }} AS dim_date
ON m.match_date = dim_date.date_day

LEFT JOIN {{ ref('dim_competitions') }} AS dim_coms
ON m.competition_id = dim_coms.competition_id

LEFT JOIN {{ ref('dim_seasons') }} AS dim_seasons
ON m.season_id = dim_seasons.season_id

LEFT JOIN {{ ref('dim_teams') }} AS dim_teams_home
ON m.home_team_id = dim_teams_home.team_id

LEFT JOIN {{ ref('dim_teams') }} AS dim_teams_away
ON m.away_team_id = dim_teams_away.team_id

LEFT JOIN {{ ref('dim_teams') }} AS dim_teams_winner
ON m.winner_team_id = dim_teams_winner.team_id