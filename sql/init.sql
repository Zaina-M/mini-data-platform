-- Sales Data Platform - Analytics Database Schema
-- This schema is designed for analytics workloads with proper indexing.

-- Create sales table with analytics-optimized structure
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL,
    order_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price > 0),
    order_date DATE NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    country VARCHAR(100) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT valid_total CHECK (total_amount = quantity * unit_price)
);

-- Create indexes for common analytics queries
CREATE INDEX IF NOT EXISTS idx_sales_order_date ON sales(order_date);
CREATE INDEX IF NOT EXISTS idx_sales_country ON sales(country);
CREATE INDEX IF NOT EXISTS idx_sales_product ON sales(product_name);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_ingestion ON sales(ingestion_timestamp);

-- Create composite index for time-based country analysis
CREATE INDEX IF NOT EXISTS idx_sales_date_country ON sales(order_date, country);

-- Create view for daily sales summary
CREATE OR REPLACE VIEW daily_sales_summary AS
SELECT 
    order_date,
    COUNT(*) as total_orders,
    SUM(quantity) as total_units,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT country) as countries_served
FROM sales
GROUP BY order_date
ORDER BY order_date DESC;

-- Create view for product performance
CREATE OR REPLACE VIEW product_performance AS
SELECT 
    product_name,
    COUNT(*) as total_orders,
    SUM(quantity) as total_units_sold,
    SUM(total_amount) as total_revenue,
    AVG(unit_price) as avg_unit_price,
    AVG(quantity) as avg_quantity_per_order
FROM sales
GROUP BY product_name
ORDER BY total_revenue DESC;

-- Create view for country analysis
CREATE OR REPLACE VIEW country_analysis AS
SELECT 
    country,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM sales
GROUP BY country
ORDER BY total_revenue DESC;

-- Grant permissions for analytics user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO analytics;
