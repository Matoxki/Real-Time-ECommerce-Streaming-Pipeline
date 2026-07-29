-- fact_customer_sessions.sql

-- 1. IMPORT CTE
-- Bring in the sessionized data from the intermediate layer
WITH sessionized_events AS (
    SELECT * 
    FROM ECOMMERCE_DB.dev_matoxki.int_sessions
),

-- 2. AGGREGATE BY SESSION
-- We group all the individual clicks up to the session level to calculate metrics.
session_metrics AS (
    SELECT 
        session_id,
        user_id,
        device,
        
        -- Time Metrics
        MIN(event_created_at) AS session_start_time,
        MAX(event_created_at) AS session_end_time,
        
        -- Calculate the duration of the session in minutes
        TIMESTAMPDIFF(MINUTE, MIN(event_created_at), MAX(event_created_at)) AS session_duration_minutes,
        
        -- Engagement Metrics (Counting specific event types)
        -- We use conditional aggregation (SUM CASE) to count how many times they did specific actions.
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS total_page_views,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS total_cart_adds,
        SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS total_checkouts_started,
        
        -- Financial Metrics
        -- Did they complete a purchase in this session? (1 for Yes, 0 for No)
        MAX(CASE WHEN event_type = 'purchase_complete' THEN 1 ELSE 0 END) AS is_converted_session,
        
        -- If they purchased, sum up the price of the items.
        SUM(CASE WHEN event_type = 'purchase_complete' THEN price ELSE 0 END) AS session_revenue

    FROM sessionized_events
    -- We group by the descriptive columns so the aggregate functions work correctly on the rest.
    GROUP BY 
        session_id, 
        user_id, 
        device
)

-- 3. FINAL OUTPUT
SELECT * FROM session_metrics