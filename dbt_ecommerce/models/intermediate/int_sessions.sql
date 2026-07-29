WITH staged_clicks AS (
    SELECT * 
    FROM {{ ref('stg_clicks') }}
),

-- 2. CALCULATE TIME DIFFERENCE (The LAG function)
time_differences AS (
    SELECT 
        *,
        -- LAG looks at the 'event_created_at' from the PREVIOUS row for this specific user orderd by time.
        LAG(event_created_at) OVER (
            PARTITION BY user_id 
            ORDER BY event_created_at
        ) AS previous_event_time
    FROM staged_clicks
),

-- 3. FLAG NEW SESSIONS
session_flags AS (
    SELECT 
        *,
        -- If this is the user's first click (previous_event_time is NULL), it's a new session (1).
        -- If the difference between this click and the last click is MORE than 15 minutes, it's a new session (1).
        -- Otherwise, it is same session
        CASE 
            WHEN previous_event_time IS NULL THEN 1
            WHEN TIMESTAMPDIFF(MINUTE, previous_event_time, event_created_at) >= 15 THEN 1
            ELSE 0 
        END AS is_new_session
    FROM time_differences
),

-- 4. ASSIGN UNIQUE SESSION IDs
-- Using a rolling sum of the 1s and 0s to create a grouping ID.
session_grouping AS (
    SELECT 
        *,
        -- This creates a running total. Every time it hits a '1', the number goes up.
        -- Effectively grouping all clicks from the same session under the same number.
        SUM(is_new_session) OVER (
            PARTITION BY user_id 
            ORDER BY event_created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_group_id
    FROM session_flags
)

-- 5. FINAL OUTPUT
-- Combine the user_id and the session_group_id to create a globally unique session identifier.
SELECT 
    -- MD5 acts as a hashing function to create a clean, unique alphanumeric string for the session ID
    MD5(user_id || '-' || session_group_id::STRING) AS session_id,
    event_id,
    user_id,
    event_type,
    product_id,
    price,
    device,
    event_created_at
FROM session_grouping