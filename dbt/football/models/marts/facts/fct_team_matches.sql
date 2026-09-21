{{ config(materialized='table') }}

with home_matches as (

    select
        match_key,
        match_id,
        competition_key,
        season_key,
        date_key,

        home_team_key as team_key,
        away_team_key as opponent_team_key,

        'Home' as home_away,

        home_team_score as team_score,
        away_team_score as opponent_score,

        match_status,
        match_matchday,
        match_stage,
        match_group

    from {{ ref('fct_matches') }}

),

away_matches as (

    select
        match_key,
        match_id,
        competition_key,
        season_key,
        date_key,

        away_team_key as team_key,
        home_team_key as opponent_team_key,

        'Away' as home_away,

        away_team_score as team_score,
        home_team_score as opponent_score,

        match_status,
        match_matchday,
        match_stage,
        match_group

    from {{ ref('fct_matches') }}

),

combined as (

    select * from home_matches

    union all

    select * from away_matches

),

calculated as (

    select
        *,

        case
            when match_status != 'FINISHED'
                then 'Not Played'

            when team_score is null
                or opponent_score is null
                then 'Unknown'

            when team_score > opponent_score
                then 'Win'

            when team_score < opponent_score
                then 'Loss'

            when team_score = opponent_score
                then 'Draw'

            else 'Unknown'
        end as result,

        {{ dbt_utils.generate_surrogate_key([
            'match_id',
            'team_key'
        ]) }} as team_match_key

    from combined

),

final as (

    select
        c.team_match_key,
        c.match_key,
        c.match_id,

        c.competition_key,
        c.season_key,
        c.date_key,

        c.team_key,
        c.opponent_team_key,

        team.team_name,
        opponent_team.team_name as opponent_team_name,

        d.date_day as match_date,

        c.home_away,
        c.team_score,
        c.opponent_score,

        c.result,
        c.match_status,
        c.match_matchday,
        c.match_stage,
        c.match_group

    from calculated c

    left join {{ ref('dim_teams') }} team
        on c.team_key = team.team_key

    left join {{ ref('dim_teams') }} opponent_team
        on c.opponent_team_key = opponent_team.team_key

    left join {{ ref('dim_date') }} d
        on c.date_key = d.date_key

)

select *
from final