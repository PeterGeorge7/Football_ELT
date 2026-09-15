SELECT
    DISTINCT (
        raw_data -> 'competition' ->> 'id'
    ) :: INT AS competition_id,
    raw_data -> 'competition' ->> 'name' AS competition_name,
    raw_data -> 'competition' ->> 'code' AS competition_code,
    raw_data -> 'competition' ->> 'type' AS competition_type,
    raw_data -> 'competition' ->> 'emblem' AS competition_emblem
FROM
    {{ source(
        'football',
        'scorers_raw'
    ) }}
