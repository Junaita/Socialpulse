with hashtags as (
    select * from {{ ref('stg_top_hashtags') }}
),

-- Top 10 trending hashtags with engagement tier
trending as (
    select
        rank,
        hashtag,
        total_mentions,
        avg_engagement,
        viral_count,
        avg_likes,
        avg_shares,

        -- Engagement tier based on avg engagement score
        case
            when avg_engagement >= 12000 then 'HOT'
            when avg_engagement >= 10000 then 'TRENDING'
            when avg_engagement >= 5000  then 'ACTIVE'
            else 'NORMAL'
        end as engagement_tier,

        -- Viral rate as percentage of mentions
        round(
            viral_count::numeric / nullif(total_mentions, 0) * 100, 1
        ) as viral_rate_pct,

        processed_at

    from hashtags
    where rank <= 10
)

select * from trending
order by rank
