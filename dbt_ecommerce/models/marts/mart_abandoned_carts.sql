WITH customer_sessions AS (
    SELECT * 
    FROM {{ ref('fact_customer_sessions') }}
),

-- 2. FILTERING LOGIC
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
        
        -- We calculate the time difference here (This is what was missing!)
        TIMESTAMPDIFF(MINUTE, session_end_time, CURRENT_TIMESTAMP()) AS minutes_since_last_activity
        
    FROM customer_sessions
    WHERE 
        -- Rule 1: They must have added at least one item to their cart
        total_cart_adds > 0
        
        -- Rule 2: They must NOT have completed a purchase
        AND total_checkouts_started = 0 
)

-- 3. FINAL OUTPUT
SELECT * 
FROM abandoned_carts
-- We keep the filter at 1 minute, and removed the upper bound so older data appears
WHERE minutes_since_last_activity >= 1