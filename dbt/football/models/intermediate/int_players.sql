WITH team_players AS (

    SELECT
        (squad ->> 'id')::INT AS player_id,
        squad ->> 'name' AS player_name,
        squad ->> 'dateOfBirth' AS player_dateOfBirth,
        squad ->> 'nationality' AS player_nationality

    FROM {{ source('football', 'teams_raw') }} AS t

    CROSS JOIN LATERAL jsonb_array_elements(
        raw_data -> 'teams'
    ) AS teams

    CROSS JOIN LATERAL jsonb_array_elements(
        teams -> 'squad'
    ) AS squad
    
), scorers AS (
        SELECT
        (scorer -> 'player' ->> 'id')::INT AS player_id,
        scorer -> 'player' ->> 'firstName' AS player_firstname,
        scorer -> 'player' ->> 'lastName' AS player_lastname,
        scorer -> 'player' ->> 'dateOfBirth' AS player_dateOfBirth,
        scorer -> 'player' ->> 'nationality' AS player_nationality 

        FROM
            {{ source(
                'football',
                'scorers_raw'
            ) }},
            LATERAL jsonb_array_elements(
                raw_data -> 'scorers'
            ) AS scorer

        WHERE NOT EXISTS (
            SELECT 1
            FROM team_players t1
            WHERE t1.player_id = (scorer -> 'player' ->> 'id')::INT
        )
)


SELECT
    tp.player_id,
    tp.player_name,
    tp.player_dateOfBirth,
    tp.player_nationality

FROM team_players AS tp

UNION 

SELECT
    s.player_id,
    CONCAT(s.player_firstname,' ', s.player_lastname) as player_name,
    s.player_dateOfBirth,
    s.player_nationality

FROM scorers AS s

{# WHERE NOT EXISTS (
    SELECT 1
    FROM team_players ts
    WHERE ts.player_id = s.player_id
) #}
