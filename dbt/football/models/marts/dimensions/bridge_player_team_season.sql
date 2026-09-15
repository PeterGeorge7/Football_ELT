SELECT
    {{ dbt_utils.generate_surrogate_key([
        'p.player_key',
        't.team_key',
        's.season_id',
        'c.competition_key'
    ]) }} AS player_team_season_key,
    p.player_key,
    t.team_key,
    s.season_key,
    c.competition_key,
    pts.player_section,
    pts.player_position,
    pts.player_shirt_number,
    pts.relationship_source
FROM {{ ref('int_player_team_season') }} pts

LEFT JOIN {{ ref('dim_players') }} as p
ON pts.player_id = p.player_id

LEFT JOIN {{ ref('dim_teams') }} as t
ON pts.team_id = t.team_id

LEFT JOIN {{ ref('dim_seasons') }} as s
ON pts.season_id = s.season_id

LEFT JOIN {{ ref('dim_competitions') }} as c
ON pts.competition_id = c.competition_id
