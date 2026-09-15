SELECT
    
    (scorer -> 'player' ->> 'id')::INT AS player_id,
    (scorer -> 'team' ->> 'id')::INT AS team_id,
    (raw_data -> 'season' ->> 'id')::INT AS season_id,
    (raw_data -> 'competition' ->> 'id')::INT AS competition_id,

    (scorer ->> 'playedMatches')::numeric AS played_matches,
    (scorer ->> 'goals')::numeric AS goals,
    (scorer ->> 'assists')::numeric AS assists,
    (scorer ->> 'penalties')::numeric AS penalties
    
    {# (COALESCE((scorer ->> 'goals') :: numeric, 0) + COALESCE((scorer ->> 'assists') :: numeric, 0)) / NULLIF((scorer ->> 'playedMatches') :: numeric, 0) AS gpg #}
FROM
    {{ source(
        'football',
        'scorers_raw'
    ) }},
    LATERAL jsonb_array_elements(
        raw_data -> 'scorers'
    ) AS scorer

{# raw_data -> 'filters' ->> 'season' AS season, #}
    {# scorer -> 'player' ->> 'firstName' AS player_firstname,
    scorer -> 'player' ->> 'lastName' AS player_lastname,
    scorer -> 'player' ->> 'dateOfBirth' AS player_dateOfBirth,
    scorer -> 'player' ->> 'nationality' AS player_nationality,
    scorer -> 'player' ->> 'section' AS player_section,
    scorer -> 'player' ->> 'position' AS player_position,
    scorer -> 'player' ->> 'shirtNumber' AS player_shirtNumber, #}
    {# scorer -> 'team' ->> 'name' AS team_name,
    scorer -> 'team' ->> 'tla' AS team_tla,
    scorer -> 'team' ->> 'crest' AS team_crest,
    scorer -> 'team' ->> 'address' AS team_address,
    scorer -> 'team' ->> 'website' AS team_website,
    scorer -> 'team' ->> 'founded' AS team_founded,
    scorer -> 'team' ->> 'clubColors' AS team_clubColors,
    scorer -> 'team' ->> 'venue' AS team_venue, #}