SELECT
    DISTINCT 
    (raw_data -> 'season' ->> 'id')::INT AS season_id,
    raw_data -> 'filters' ->> 'season' AS season_year,
    raw_data -> 'season' ->> 'startDate' AS season_start_date,
    raw_data -> 'season' ->> 'endDate' AS season_end_date,
    raw_data -> 'season' ->> 'currentMatchday' AS season_current_matchday
FROM
    {{ source(
        'football',
        'scorers_raw'
    ) }}
