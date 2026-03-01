# Dashboard Requirements

This document specifies the Metabase dashboards to be created for the Sales Data Platform.

---

## Dashboard Overview

The dashboards provide business intelligence capabilities for sales performance monitoring. Each dashboard serves a specific analytical purpose and answers key business questions.

---

## Dashboard 1: Sales Over Time

### Purpose
Track revenue trends and identify seasonality, growth patterns, and anomalies.

### Business Questions Answered
- Is revenue growing or declining?
- Are there seasonal patterns in our sales?
- Did a recent campaign impact sales?
- Are there any unexpected drops that need investigation?

### Recommended Visualizations

**Primary Chart: Line Chart**
- X-axis: Date (order_date)
- Y-axis: Total Revenue (SUM of total_amount)
- Granularity: Daily or Weekly (configurable)

**Secondary Chart: Bar Chart**
- X-axis: Month
- Y-axis: Total Revenue
- Purpose: Monthly comparison

### SQL Query Example
```sql
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue
FROM sales
WHERE order_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY order_date
ORDER BY order_date;
```

### Why This Dashboard Exists
Revenue trends are the most fundamental business metric. Executives check this daily to understand business health. Sudden changes trigger investigations into marketing, product, or operational issues.

---

## Dashboard 2: Sales by Country

### Purpose
Understand geographic distribution of revenue and identify market opportunities.

### Business Questions Answered
- Which countries generate the most revenue?
- Are we growing in target markets?
- Which regions are underperforming?
- Should we invest in localization for specific markets?

### Recommended Visualizations

**Primary Chart: Horizontal Bar Chart**
- Y-axis: Country
- X-axis: Total Revenue
- Sorted: Descending by revenue

**Secondary Chart: Map Visualization** (if Metabase supports)
- Color intensity: Revenue volume
- Purpose: Visual geographic distribution

**Table View**
- Columns: Country, Order Count, Total Revenue, Avg Order Value
- Purpose: Detailed comparison

### SQL Query Example
```sql
SELECT 
    country,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM sales
GROUP BY country
ORDER BY total_revenue DESC;
```

### Why This Dashboard Exists
Geographic analysis drives expansion decisions. A country with high order volume but low average order value might need premium product focus. A country with few orders but high value might benefit from increased marketing.

---

## Dashboard 3: Top Products

### Purpose
Identify best-selling products and inform inventory and marketing decisions.

### Business Questions Answered
- Which products generate the most revenue?
- Which products sell in highest quantities?
- What's our product mix?
- Are there products we should discontinue or promote?

### Recommended Visualizations

**Primary Chart: Horizontal Bar Chart**
- Y-axis: Product Name
- X-axis: Total Revenue
- Limit: Top 10 products

**Secondary Chart: Pie/Donut Chart**
- Segments: Product categories (if available)
- Purpose: Revenue distribution

**Table View**
- Columns: Product Name, Units Sold, Total Revenue, Avg Unit Price
- Purpose: Detailed product analysis

### SQL Query Example
```sql
SELECT 
    product_name,
    SUM(quantity) as units_sold,
    SUM(total_amount) as total_revenue,
    AVG(unit_price) as avg_unit_price
FROM sales
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10;
```

### Why This Dashboard Exists
Product performance informs multiple business decisions:
- Inventory management (stock more of what sells)
- Marketing (promote high-margin products)
- Product development (identify gaps in catalog)
- Pricing (adjust prices based on demand)

---

## Dashboard 4: Average Order Value (AOV)

### Purpose
Track customer spending patterns and measure the effectiveness of upselling strategies.

### Business Questions Answered
- How much do customers spend per order on average?
- Is AOV increasing or decreasing over time?
- Which segments have higher AOV?
- Are bundling strategies working?

### Recommended Visualizations

**Primary Chart: Single Number Card**
- Value: Current Period AOV
- Comparison: Previous period (trend indicator)

**Secondary Chart: Line Chart**
- X-axis: Date
- Y-axis: Daily Average Order Value
- Purpose: Trend over time

**Supplementary Chart: Bar Chart by Country**
- Y-axis: Country
- X-axis: Average Order Value
- Purpose: Geographic AOV comparison

### SQL Query Example
```sql
-- Overall AOV
SELECT 
    AVG(total_amount) as average_order_value,
    COUNT(*) as total_orders
FROM sales;

-- AOV Trend
SELECT 
    order_date,
    AVG(total_amount) as daily_aov,
    COUNT(*) as order_count
FROM sales
GROUP BY order_date
ORDER BY order_date;
```

### Why This Dashboard Exists
AOV is a key e-commerce metric. It's easier to increase revenue by increasing AOV (through upselling, bundling, or premium products) than by acquiring new customers. Tracking AOV helps measure the success of these strategies.

---

## Dashboard Implementation Guide

### Step 1: Connect Metabase to PostgreSQL

1. Access Metabase at http://localhost:3000
2. Complete initial setup (admin account creation)
3. Add database connection:
   - Database type: PostgreSQL
   - Host: analytics-db
   - Port: 5432
   - Database: analytics
   - Username: analytics
   - Password: analytics

### Step 2: Create Questions (Queries)

For each dashboard component:
1. Click "New" -> "Question"
2. Select "Native query" for SQL or "Simple question" for GUI
3. Write the query or select fields
4. Choose visualization type
5. Save with descriptive name

### Step 3: Build Dashboards

1. Click "New" -> "Dashboard"
2. Name the dashboard (e.g., "Sales Overview")
3. Click "Add" to add saved questions
4. Arrange cards in logical order
5. Add filter widgets for date range, country, etc.
6. Save dashboard

### Step 4: Configure Auto-Refresh

1. Open dashboard
2. Click clock icon
3. Set refresh interval (e.g., 15 minutes)
4. Dashboards will automatically update

---

## Filter Recommendations

Apply these filters across dashboards for interactivity:

| Filter | Type | Purpose |
|--------|------|---------|
| Date Range | Date picker | Focus on specific period |
| Country | Dropdown | Regional analysis |
| Product | Dropdown | Product-specific deep dive |

---

## Performance Considerations

For optimal dashboard performance:

1. **Use database views** for complex aggregations
2. **Leverage indexes** created in init.sql
3. **Cache queries** in Metabase settings
4. **Limit date ranges** for large datasets
5. **Aggregate data** for trend charts (daily/weekly vs. hourly)

---

## Future Enhancements

Consider adding these dashboards as the platform matures:

1. **Customer Cohort Analysis** - Track repeat purchase behavior
2. **Inventory Dashboard** - Stock levels and reorder points
3. **Sales Team Performance** - If sales rep data is added
4. **Funnel Analysis** - Conversion rates across stages
5. **Anomaly Detection** - Automated alerting on unusual patterns
