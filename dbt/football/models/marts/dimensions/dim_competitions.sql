SELECT 
    {{dbt_utils.generate_surrogate_key(['competition_id'])}} as competition_key,
    competition_id,
    competition_name,
    competition_code,
    competition_type,
    competition_emblem
FROM {{ ref('int_competitions') }}