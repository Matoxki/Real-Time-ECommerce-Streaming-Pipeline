-- We pull directly from our sessionized fact table, as it already contains all the heavy lifting
WITH customer_sessions AS (
    SELECT * 
    FROM ECOMMERCE_DB.dev_matoxki.fact_customer_sessions
),

-- 2. FILTERING LOGIC
-- We isolate sessions that meet the strict business criteria for an "abandoned cart"
abandoned_carts AS (
    SELECT 
        session_id,
        user_id,
        device,
        session_start_time,
        session_end_time,
        session_duration_minutes,
        total_cart_adds,
        session_revenue,
        
        -- Calculate how much time has passed since the user's last interaction in this session
        TIMESTAMPDIFF(MINUTE, session_end_time, CURRENT_TIMESTAMP()) AS minutes_since_last_activity
        
    FROM customer_sessions
    WHERE 
        -- Rule 1: They must have added at least one item to their cart
        total_cart_adds > 0
        
        -- Rule 2: They must NOT have completed a purchase
        AND total_checkouts_started = 0 -- (or use is_converted_session = 0 if you have that flag)
)

-- 3. FINAL OUTPUT
-- We only output carts that have been sitting idle for 15 minutes or more.
-- This creates a live "Trigger Table" ready to be consumed by marketing systems (Reverse-ETL).
SELECT * 
FROM abandoned_carts
WHERE minutes_since_last_activity >= 15