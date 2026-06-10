with source as (
    select * from {{ source('socialpulse', 'raw_top_hashtags') }}
),

renamed as (
    select
        hashtag,
        total_mentions,
        avg_likes,
        avg_shares,
        avg_comments,
        avg_engagement,
        viral_count,
        rank,
        processed_at
    from source
)

select * from renamed
