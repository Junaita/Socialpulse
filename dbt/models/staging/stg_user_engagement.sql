with source as (
    select * from {{ source('socialpulse', 'raw_user_engagement') }}
),

renamed as (
    select
        user_id,
        username,
        total_events,
        total_posts,
        total_likes,
        total_shares,
        total_comments,
        total_engagement_score,
        avg_engagement_score,
        viral_posts,
        devices_used,
        primary_location,
        user_rank,
        processed_at
    from source
)

select * from renamed
