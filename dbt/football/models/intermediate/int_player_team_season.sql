WITH team_players AS (

    SELECT
        DISTINCT
        (squad ->> 'id')::INT AS player_id,
        (team ->> 'id')::INT as team_id,
        (t.raw_data -> 'season' ->> 'id'):: INT as season_id,
        (t.raw_data -> 'competition' -> 'id')::INT as competition_id,
        squad ->> 'position' AS player_position,
        TRUE AS from_squad

    FROM {{ source('football', 'teams_raw') }} AS t

    CROSS JOIN LATERAL jsonb_array_elements(
        t.raw_data -> 'teams'
    ) AS team

    CROSS JOIN LATERAL jsonb_array_elements(
        team -> 'squad'
    ) AS squad

), 
scorer_players AS (

    SELECT
        DISTINCT
        (scorer -> 'player' ->> 'id')::INT AS player_id,
        (scorer -> 'team' ->> 'id') ::INT as team_id,
        (raw_data -> 'season' ->> 'id') ::INT AS season_id,
        (s.raw_data -> 'competition' -> 'id')::INT as competition_id,
        scorer -> 'player' ->> 'section' AS player_section,
        scorer -> 'player' ->> 'position' AS player_position,
        (scorer -> 'player' ->> 'shirtNumber')::INT AS player_shirt_number,
        TRUE AS from_scorers

    FROM {{ source('football', 'scorers_raw') }} AS s

    CROSS JOIN LATERAL jsonb_array_elements(
        raw_data -> 'scorers'
    ) AS scorer
), player_team_season AS (

    SELECT
        player_id,
        team_id,
        season_id,
        competition_id
    FROM team_players

    UNION

    SELECT
        player_id,
        team_id,
        season_id,
        competition_id
    FROM scorer_players
)

SELECT
    pts.player_id,
    pts.team_id,
    pts.season_id,
    pts.competition_id,
    sr.player_section,

    COALESCE(
        sq.player_position,
        sr.player_position
    ) AS player_position,

    sr.player_shirt_number,

    CASE
        WHEN sq.player_id IS NOT NULL 
            AND sr.player_id IS NOT NULL
        THEN 'both'

        WHEN sq.player_id IS NOT NULL
        THEN 'squad'

        WHEN sr.player_id IS NOT NULL
        THEN 'scorers'
    END AS relationship_source

FROM player_team_season AS pts

LEFT JOIN team_players AS sq
    ON pts.player_id = sq.player_id
    AND pts.team_id = sq.team_id
    AND pts.season_id = sq.season_id
    AND pts.competition_id = sq.competition_id

LEFT JOIN scorer_players AS sr
    ON pts.player_id = sr.player_id
    AND pts.team_id = sr.team_id
    AND pts.season_id = sr.season_id
    AND pts.competition_id = sr.competition_id



{# SELECT
    tp.player_id,
    tp.team_id,
    

    COALESCE(
        tp.player_position,
        s.player_position,
        s.player_section
        ) AS player_position,
    s.player_shirt_number:: INT AS player_shirt_Number,
    tp.season_id

FROM team_players AS tp
LEFT JOIN scorer_players as s
    ON tp.player_id = s.player_id
    AND tp.team_id = s.team_id
    AND tp.season_id = s.season_id


UNION

SELECT
    s.player_id,
    s.team_id,
    COALESCE(
        s.player_section,
        s.player_position),
    s.player_shirt_number,
    s.season_id

FROM scorer_players AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM team_players tp
    WHERE tp.player_id = s.player_id
    AND tp.team_id = s.team_id
    AND tp.season_id = s.season_id
) #}