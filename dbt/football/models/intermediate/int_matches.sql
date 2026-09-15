SELECT
    (matches -> 'competition' ->> 'id')::INT AS competition_id,
    (matches -> 'season' ->> 'id')::INT AS season_id,
    (matches ->> 'id')::INT AS match_id,
    (matches ->> 'utcDate')::timestamptz::DATE AS match_date,
    (matches ->> 'utcDate')::timestamptz AS match_datetime,
    matches ->> 'status' AS match_status,
    (matches ->> 'matchday')::int AS match_matchday,
    matches ->> 'stage' AS match_stage,
    matches ->> 'group' AS match_group,
    (matches -> 'homeTeam' ->> 'id')::INT AS home_team_id,
    (matches -> 'awayTeam' ->> 'id')::INT AS away_team_id,
    matches -> 'score' ->> 'winner' AS match_result,
    CASE 
        WHEN matches -> 'score' ->> 'winner' = 'HOME_TEAM' THEN (matches -> 'homeTeam' ->> 'id')::INT
        WHEN matches -> 'score' ->> 'winner' = 'AWAY_TEAM' THEN (matches -> 'awayTeam' ->> 'id')::INT
        ELSE NULL 
    END AS winner_team_id,
    matches -> 'score' ->> 'duration' AS duration,
    (matches -> 'score' -> 'fullTime' ->> 'home')::INT AS home_team_score,
    (matches -> 'score' -> 'fullTime' ->> 'away')::INT AS away_team_score


FROM {{ source('football', 'matches_raw') }}

CROSS JOIN LATERAL jsonb_array_elements(
        raw_data -> 'matches'
    ) AS matches
