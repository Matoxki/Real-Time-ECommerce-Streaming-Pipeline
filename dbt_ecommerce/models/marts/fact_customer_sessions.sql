{{ config(materialized='view') }}

WITH sessionized_events AS (
    SELECT
        session_id,
        event_id,
        user_id,
        event_type,
        product_id,
        price,
        device,
        event_created_at
    FROM {{ ref('int_sessions') }}
),

session_metrics AS (
    SELECT
        session_id,
        user_id,

        -- Use the device from the first event in the session.
        -- Do not GROUP BY device because the simulated device may vary per event.
        MIN_BY(device, event_created_at) AS device,

        MIN(event_created_at) AS session_start_time,
        MAX(event_created_at) AS session_end_time,

        TIMESTAMPDIFF(
            MINUTE,
            MIN(event_created_at),
            MAX(event_created_at)
        ) AS session_duration_minutes,

        COUNT_IF(event_type = 'page_view') AS total_page_views,
        COUNT_IF(event_type = 'add_to_cart') AS total_cart_adds,
        COUNT_IF(event_type = 'checkout_start') AS total_checkouts_started,

        MAX(
            IFF(event_type = 'purchase_complete', 1, 0)
        ) AS is_converted_session,

        SUM(
            IFF(
                event_type = 'purchase_complete',
                COALESCE(price, 0),
                0
            )
        ) AS session_revenue

    FROM sessionized_events
    GROUP BY
        session_id,
        user_id
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
    is_converted_session,
    session_revenue
FROM session_metrics