SELECT
    (matches ->> 'id')::INT AS match_id,
    (refrees ->> 'id')::INT AS refree_id,
    refrees ->> 'name' AS refree_name,
    refrees ->> 'nationality' AS refree_nationality


FROM {{ source('football', 'matches_raw') }}

CROSS JOIN LATERAL jsonb_array_elements(
        raw_data -> 'matches'
    ) AS matches

CROSS JOIN LATERAL jsonb_array_elements(
        matches -> 'referees'
    ) AS refrees

