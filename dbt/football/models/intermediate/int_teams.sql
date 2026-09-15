SELECT
    DISTINCT (
        teams ->> 'id'
    ) :: INT AS team_id,
    (
        teams -> 'area' ->> 'id'
    ) :: INT AS area_id,
    teams -> 'area' ->> 'name' AS area_name,
    teams -> 'area' ->> 'code' AS area_code,
    teams -> 'area' ->> 'flag' AS area_flag,
    teams ->> 'name' AS team_name,
    teams ->> 'shortName' AS team_short_name,
    teams ->> 'tla' AS team_tla,
    teams ->> 'crest' AS team_crest,
    teams ->> 'address' AS team_address,
    teams ->> 'website' AS team_website,
    teams ->> 'founded' AS team_founded,
    teams ->> 'clubColors' AS team_club_colors,
    teams ->> 'venue' AS team_venue
FROM
    {{ source(
        'football',
        'teams_raw'
    ) }},
    LATERAL jsonb_array_elements(
        raw_data -> 'teams'
    ) AS teams
ORDER BY
    team_id ASC
