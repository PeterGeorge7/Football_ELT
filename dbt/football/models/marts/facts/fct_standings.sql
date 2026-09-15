SELECT
    dim_teams.team_key,
    dim_coms.competition_key,
    dim_seasons.season_key,

    s.stage,
    s.type,

    s.team_position,
    s.form,

    s.played_games,
    s.won,
    s.draw,
    s.lost,
    s.points,
    s.goals_for,
    s.goals_against,
    s.goals_difference
    
FROM {{ ref('int_standings') }} as s

LEFT JOIN {{ ref('dim_competitions') }} AS dim_coms
ON s.competition_id = dim_coms.competition_id

LEFT JOIN {{ ref('dim_seasons') }} AS dim_seasons
ON s.season_id = dim_seasons.season_id

LEFT JOIN {{ ref('dim_teams') }} AS dim_teams
ON s.team_id = dim_teams.team_id