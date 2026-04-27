CREATE TABLE sessions (
    session_id INT PRIMARY KEY,
    user_id INT,
    session_start TIMESTAMP,
    session_duration_sec INT,
    pages_viewed INT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

DROP TABLE sessions 
ALTER COLUMN session_duration_sec TYPE FLOAT;

CREATE TABLE sessions (
    session_id TEXT,
    user_id TEXT,
    session_start TEXT,
    session_duration_sec TEXT,
    pages_viewed TEXT
);

SELECT COUNT(*) FROM sessions

ALTER TABLE sessions
ALTER COLUMN session_id TYPE INT USING session_id::INT,
ALTER COLUMN user_id TYPE INT USING user_id::INT,
ALTER COLUMN session_start TYPE TIMESTAMP USING session_start::TIMESTAMP,
ALTER COLUMN session_duration_sec TYPE INT USING session_duration_sec::FLOAT,
ALTER COLUMN pages_viewed TYPE INT USING pages_viewed:: FLOAT;

CREATE TABLE payments (
    payment_id INT PRIMARY KEY,
    user_id INT,
    amount FLOAT,
    payment_status TEXT,
    payment_date TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

SELECT * FROM payments

CREATE TABLE events (
    event_id INT PRIMARY KEY,
	user_id INT,
	event_type VARCHAR(30),
	event_time TIMESTAMP,
	FOREIGN KEY (user_id) REFERENCES users(user_id)
);

SELECT * FROM events

-- Distinct users across tables
SELECT COUNT(DISTINCT user_id) FROM users;
SELECT COUNT(DISTINCT user_id) FROM events;
SELECT COUNT(DISTINCT user_id) FROM payments;

-- event taxonomy check
SELECT event_type, COUNT(*) 
FROM events
GROUP BY event_type
ORDER BY COUNT(*) DESC;

-- base funnel counts
SELECT
    COUNT(DISTINCT CASE WHEN event_type = 'signup' THEN user_id END) AS signup,
    COUNT(DISTINCT CASE WHEN event_type = 'onboarding_complete' THEN user_id END) AS onboarding,
    COUNT(DISTINCT CASE WHEN event_type = 'trial_start' THEN user_id END) AS trial,
    COUNT(DISTINCT CASE WHEN event_type = 'payment' THEN user_id END) AS payment
FROM events;


SELECT DISTINCT event_type
FROM events
ORDER BY event_type;

--conversion rates
WITH funnel AS (
    SELECT
        COUNT(DISTINCT CASE WHEN e.event_type = 'signup' THEN e.user_id END) AS signup,
        COUNT(DISTINCT CASE WHEN e.event_type = 'onboarding_complete' THEN e.user_id END) AS onboarding,
        COUNT(DISTINCT CASE WHEN e.event_type = 'trial_start' THEN e.user_id END) AS trial,
        COUNT(DISTINCT CASE WHEN p.payment_status = 'success' THEN p.user_id END) AS payment
    FROM events e
    LEFT JOIN payments p ON e.user_id = p.user_id
)

SELECT *,
    onboarding * 1.0 / signup AS signup_to_onboarding,
    trial * 1.0 / onboarding AS onboarding_to_trial,
    payment * 1.0 / trial AS trial_to_payment
FROM funnel;


-- Time to Conversion
WITH signup AS (
    SELECT user_id, MIN(event_time) AS signup_time
    FROM events
    WHERE event_type = 'signup'
    GROUP BY user_id
),
payment AS (
    SELECT user_id, MIN(payment_date) AS payment_time
    FROM payments
    WHERE payment_status = 'success'
    GROUP BY user_id
)

SELECT 
    AVG(payment_time - signup_time) AS avg_time_to_convert
FROM signup s
JOIN payment p ON s.user_id = p.user_id;


-- Segment Analysis (Device / Source / Country)
SELECT 
    u.device,
    COUNT(DISTINCT CASE WHEN e.event_type = 'signup' THEN u.user_id END) AS total_users,
    COUNT(DISTINCT CASE WHEN p.payment_status = 'success' THEN u.user_id END) AS converted_users,
    COUNT(DISTINCT CASE WHEN p.payment_status = 'success' THEN u.user_id END) * 1.0 /
    COUNT(DISTINCT CASE WHEN e.event_type = 'signup' THEN u.user_id END) AS conversion_rate
FROM users u
LEFT JOIN events e ON u.user_id = e.user_id
LEFT JOIN payments p ON u.user_id = p.user_id
GROUP BY u.device
ORDER BY conversion_rate;


-- Behavioral Analysis
WITH user_behavior AS (
    SELECT 
        u.user_id,
        COUNT(s.session_id) AS session_count,
        AVG(s.session_duration_sec) AS avg_duration,
        SUM(s.pages_viewed) AS total_pages,
        MAX(CASE WHEN p.payment_status = 'success' THEN 1 ELSE 0 END) AS converted
    FROM users u
    LEFT JOIN sessions s ON u.user_id = s.user_id
    LEFT JOIN payments p ON u.user_id = p.user_id
    GROUP BY u.user_id
)

SELECT 
    CASE 
        WHEN session_count >= 3 THEN 'high_engagement'
        ELSE 'low_engagement'
    END AS segment,
    COUNT(*) AS users,
    SUM(converted) AS converted_users,
    SUM(converted) * 1.0 / COUNT(*) AS conversion_rate
FROM user_behavior
GROUP BY segment;



-- Revenue Validation
SELECT 
    SUM(CASE WHEN payment_status = 'success' THEN amount ELSE 0 END) AS total_revenue,
    COUNT(*) FILTER (WHERE payment_status = 'failed') AS failed_payments,
    COUNT(*) FILTER (WHERE payment_status = 'pending') AS pending_payments
FROM payments;



-- Revenue by Segment
-- by traffic source
SELECT 
    u.traffic_source,
    SUM(p.amount) AS revenue,
    COUNT(DISTINCT p.user_id) AS customers,
    AVG(p.amount) AS avg_ticket
FROM payments p
JOIN users u ON p.user_id = u.user_id
WHERE p.payment_status = 'success'
GROUP BY u.traffic_source
ORDER BY revenue DESC;


-- by Device
SELECT 
    u.device,
    SUM(p.amount) AS revenue,
    COUNT(DISTINCT p.user_id) AS customers,
    AVG(p.amount) AS avg_ticket
FROM payments p
JOIN users u ON p.user_id = u.user_id
WHERE p.payment_status = 'success'
GROUP BY u.device
ORDER BY revenue DESC;

