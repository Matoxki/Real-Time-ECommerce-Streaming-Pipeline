{{ config(materialized='view') }}

WITH customer_sessions AS (
    SELECT
        session_id,
        user_id,
        device,
        session_start_time,
        session_end_time,
        session_duration_minutes,
        total_page_views,
        total_cart_adds,
        total_checkouts_started,
        is_converted_session,
        session_revenue
    FROM {{ ref('fact_customer_sessions') }}
),

candidate_carts AS (
    SELECT
        session_id,
        user_id,
        device,
        session_start_time,
        session_end_time,
        session_duration_minutes,
        total_page_views,
        total_cart_adds,
        total_checkouts_started,
        is_converted_session,
        session_revenue,

        TIMESTAMPDIFF(
            MINUTE,
            session_end_time,
            CURRENT_TIMESTAMP()
        ) AS minutes_since_last_activity

    FROM customer_sessions
    WHERE
        total_cart_adds > 0
        AND is_converted_session = 0
)

SELECT
    session_id,
    user_id,
    device,
    session_start_time,
    session_end_time,
    session_duration_minutes,
    total_page_views,
    total_cart_adds,
    total_checkouts_started,
    session_revenue,
    minutes_since_last_activity
FROM candidate_carts
WHERE minutes_since_last_activity >= 15