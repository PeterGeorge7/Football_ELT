select 
    p.player_key,
    t.team_key,
    s.season_key,
    c.competition_key,

    played_matches,
    goals,
    assists,
    penalties,
    (
        COALESCE(goals, 0) + 
        COALESCE(assists, 0)
    ) / 
        NULLIF(played_matches :: numeric, 0) AS goals_and_assits_ratio
    
from {{ ref('int_scorers') }} as sc

left join {{ ref('dim_players') }} as p
ON sc.player_id = p.player_id

left join {{ ref('dim_teams') }} as t
ON sc.team_id = t.team_id

left join {{ ref('dim_seasons') }} as s
ON sc.season_id = s.season_id

left join {{ ref('dim_competitions') }} as c
ON sc.competition_id = c.competition_id