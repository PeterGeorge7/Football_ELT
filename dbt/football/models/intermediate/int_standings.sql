SELECT 
    (table_s -> 'team' ->> 'id')::INT AS team_id,
    (raw_data -> 'competition' ->> 'id')::INT AS competition_id,
    (raw_data -> 'season' ->> 'id')::INT AS season_id,

    standings ->> 'stage' AS stage,
    standings ->> 'type' AS type,

    (table_s ->> 'position')::INT AS team_position,
    (table_s ->> 'playedGames')::INT AS played_games,
    table_s ->> 'form' AS form,

    (table_s ->> 'won')::int AS won,
    (table_s ->> 'draw')::int AS draw,
    (table_s ->> 'lost')::int AS lost,
    (table_s ->> 'points')::int AS points,
    (table_s ->> 'goalsFor')::int AS goals_for,
    (table_s ->> 'goalsAgainst')::int AS goals_against,
    (table_s ->> 'goalDifference')::int AS goals_difference


FROM {{ source('football', 'standings_raw') }}

CROSS JOIN LATERAL jsonb_array_elements(raw_data -> 'standings') AS standings

CROSS JOIN LATERAL jsonb_array_elements(standings -> 'table') AS table_s