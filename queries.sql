-- ============================================================
-- Name: Richard Pallangyo
-- ID: rap7777
-- Database: capstone_db_rap7777
-- ============================================================

-- Query 1: Product Conversion Funnel Analysis
-- Purpose: Analyze conversion rates from view → cart → purchase
SELECT 
    product_id,
    category,
    view_count,
    add_to_cart_count,
    purchase_count,
    ROUND(view_to_cart_rate * 100, 2) as view_to_cart_pct,
    ROUND(cart_to_purchase_rate * 100, 2) as cart_to_purchase_pct,
    ROUND(view_to_purchase_rate * 100, 2) as overall_conversion_pct
FROM product_funnel
ORDER BY view_count DESC
LIMIT 20;

-- Query 2: Hourly Revenue Trends
-- Purpose: Track revenue patterns by hour to identify peak sales periods
SELECT 
    event_date,
    event_hour,
    ROUND(total_revenue, 2) as total_revenue,
    purchase_count,
    ROUND(avg_order_value, 2) as avg_order_value,
    total_items_sold
FROM hourly_revenue
ORDER BY event_date DESC, event_hour DESC;

-- Query 3: Top 10 Most Popular Products
-- Purpose: Identify products with highest customer engagement
SELECT 
    product_id,
    category,
    view_count,
    unique_viewers,
    ROUND(CAST(view_count AS DOUBLE) / CAST(unique_viewers AS DOUBLE), 2) as views_per_user
FROM product_popularity
ORDER BY view_count DESC
LIMIT 10;

-- Query 4: Daily Category Performance
-- Purpose: Compare category performance across different metrics
SELECT 
    event_date,
    category,
    total_events,
    page_views,
    add_to_carts,
    purchases,
    ROUND(total_revenue, 2) as revenue,
    ROUND(CAST(purchases AS DOUBLE) / CAST(page_views AS DOUBLE) * 100, 2) as conversion_rate_pct
FROM category_daily_performance
ORDER BY event_date DESC, total_revenue DESC;

-- Query 5: Daily User Activity and Engagement
-- Purpose: Monitor user engagement trends over time
SELECT 
    event_date,
    unique_users,
    unique_sessions,
    total_events,
    ROUND(events_per_user, 2) as avg_events_per_user,
    ROUND(sessions_per_user, 2) as avg_sessions_per_user,
    ROUND(CAST(total_events AS DOUBLE) / CAST(unique_sessions AS DOUBLE), 2) as events_per_session
FROM daily_user_activity
ORDER BY event_date DESC;
