

# E-Commerce Analytics Pipeline

![High Level Architecture Diagram](./architecture-diagram.png)

## Architecture Overview

This project implements a **Medallion Architecture** pipeline (Bronze -> Silver -> Gold) using AWS Glue, S3, and Athena to process high-volume e-commerce event data.

  * **Bronze Layer (Raw):** Ingests raw JSONL.gz events with Hive-style partitioning.
  * **Silver Layer (Enriched):** Cleaned and enriched Parquet files with timestamp parsing and revenue calculation.
  * **Gold Layer (Aggregated):** 5 pre-aggregated analytics tables optimized for high-performance querying.
  * **Incremental Processing:** Utilizes AWS Glue job bookmarks to track processed files and prevent data duplication.

-----

## Deployment & Testing Guide

### 1\. Deploy CloudFormation Stack

Deploys the infrastructure, including S3 buckets, Glue Databases/Crawlers, and Lambda functions.

```bash
aws cloudformation deploy \
  --template-file capstone-analytics-pipeline-rap7777.cfn.yaml \
  --stack-name capstone-pipeline \
  --parameter-overrides StudentId=<student-id> \
  --region us-west-2
```

> **Creates:**
>
>   * **S3 Bucket:** `capstone-data-<student-id>-<account-id>`
>   * **Lambda:** Event generator
>   * **Glue DB:** `capstone_db_<student-id>`
>   * **Crawlers:** 3 (Bronze, Silver, Gold)
>   * **ETL Job:** PySpark with bookmarks enabled
>   * **Athena Workgroup:** `capstone-analytics-<student-id>`

### 2\. Verify Stack Outputs

Check that the stack deployed successfully and view the created resources.

```bash
aws cloudformation describe-stacks \
  --stack-name capstone-pipeline \
  --region us-west-2 \
  --query 'Stacks[0].Outputs' \
  --output table
```

### 3\. Upload ETL Script

Fetch the created bucket name and upload the PySpark logic.

```bash
# Get bucket name from stack outputs
BUCKET=$(aws cloudformation describe-stacks \
  --stack-name capstone-pipeline \
  --region us-west-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
  --output text)

echo "Bucket: $BUCKET"

# Upload PySpark script
aws s3 cp etl_script.py s3://${BUCKET}/capstone/scripts/etl_script.py --region us-west-2
```

### 4\. Generate Sample Data

Invoke the Lambda function to simulate traffic. Each invocation generates 500K-750K events immediately.

```bash
aws lambda invoke \
  --function-name capstone-event-generator-<student-id> \
  --region us-west-2 \
  --payload '{}' \
  response.json

# Check response
cat response.json
```

> **Output:** Each invocation generates 500K-750K events written to:
> `s3://${BUCKET}/capstone/raw/events/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/`

> **Note:** The EventBridge schedule is disabled by default. To generate continuous data (recommended for at least 1 hour), enable it after deployment:
> ```bash
> aws events enable-rule --name capstone-generator-schedule-<student-id> --region us-west-2
> ```

*Alternatively, manually invoke multiple times over 1+ hours.*

**Verify data generated:**
```bash
aws s3 ls s3://${BUCKET}/capstone/raw/events/ --recursive --region us-west-2 --human-readable
```

### 5\. Catalog Bronze Data

Crawl the raw data to make it queryable.

```bash
aws glue start-crawler --name capstone-source-crawler-<student-id> --region us-west-2

# Wait 30-60 seconds, then check crawler status (should show: READY)
aws glue get-crawler --name capstone-source-crawler-<student-id> \
  --region us-west-2 \
  --query 'Crawler.State' --output text
```

**Verify Bronze table created:**
```bash
aws glue get-table \
  --database-name capstone_db_<student-id> \
  --name events \
  --region us-west-2 \
  --query 'Table.Name' \
  --output text
```

### 6\. Run ETL Job

Execute the Spark job to process Bronze -> Silver -> Gold.

```bash
# Start the job and capture the JobRunId
JOB_RUN_ID=$(aws glue start-job-run \
  --job-name capstone-etl-job-<student-id> \
  --region us-west-2 \
  --query 'JobRunId' \
  --output text)

echo "JobRunId: $JOB_RUN_ID"

# Monitor job progress (wait 2-3 minutes between checks)
aws glue get-job-run \
  --job-name capstone-etl-job-<student-id> \
  --run-id $JOB_RUN_ID \
  --region us-west-2 \
  --query 'JobRun.[JobRunState,ExecutionTime]' \
  --output json
```

> **Performance:**
>
>   * **Small dataset (10 min):** about 3-5 minutes
>   * **1 hour dataset:** about 14 minutes
>   * **2+ hour dataset:** about 20 minutes

**Verify Gold tables created:**
```bash
aws s3 ls s3://${BUCKET}/capstone/gold/ --region us-west-2
```

You should see 5 folders: `category_daily_performance/`, `daily_user_activity/`, `hourly_revenue/`, `product_funnel/`, `product_popularity/`

### 7\. Catalog Gold Tables

Update the Glue Data Catalog with the new aggregated tables.

```bash
aws glue start-crawler --name capstone-gold-crawler-<student-id> --region us-west-2

# Wait 60 seconds, then verify crawler completed
aws glue get-crawler --name capstone-gold-crawler-<student-id> \
  --region us-west-2 \
  --query 'Crawler.State' --output text

# Verify all tables created
aws glue get-tables \
  --database-name capstone_db_<student-id> \
  --region us-west-2 \
  --query 'TableList[*].Name' \
  --output json
```

**Expected Tables:**

  * `events` (Bronze)
  * `product_funnel` (Gold)
  * `hourly_revenue` (Gold)
  * `product_popularity` (Gold)
  * `category_daily_performance` (Gold)
  * `daily_user_activity` (Gold)

### 8\. Query Gold Tables

You can run queries via the AWS Console or CLI.

**Athena Console:**

  * **Workgroup:** `capstone-analytics-<student-id>`
  * **Database:** `capstone_db_<student-id>`

**AWS CLI Example:**

```bash
# Run a query
QUERY_ID=$(aws athena start-query-execution \
  --query-string "SELECT * FROM product_funnel ORDER BY view_count DESC LIMIT 10;" \
  --query-execution-context Database=capstone_db_<student-id> \
  --result-configuration OutputLocation=s3://${BUCKET}/capstone/athena-results/ \
  --work-group capstone-analytics-<student-id> \
  --region us-west-2 \
  --query 'QueryExecutionId' \
  --output text)

# Check query status and performance
aws athena get-query-execution \
  --query-execution-id $QUERY_ID \
  --region us-west-2 \
  --query 'QueryExecution.[Status.State,Statistics.[DataScannedInBytes,EngineExecutionTimeInMillis]]' \
  --output json
```

> **Expected Performance:** \<1 second per query, \<5 KB data scanned.

### 9\. Analytics Queries

The Gold layer supports 5 key business analytics questions:

#### Query 1: Product Conversion Funnel

**Business Question:** Which products have the best view-to-purchase conversion rates?

```sql
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
LIMIT 10;
```

#### Query 2: Hourly Revenue Trends

**Business Question:** What are our peak revenue hours?

```sql
SELECT 
    event_date,
    event_hour,
    ROUND(total_revenue, 2) as total_revenue,
    purchase_count,
    ROUND(avg_order_value, 2) as avg_order_value,
    total_items_sold
FROM hourly_revenue
ORDER BY event_date DESC, event_hour DESC
LIMIT 20;
```

#### Query 3: Top Products by Engagement

**Business Question:** Which products generate the most customer interest?

```sql
SELECT 
    product_id,
    category,
    view_count,
    unique_viewers,
    ROUND(CAST(view_count AS DOUBLE) / CAST(unique_viewers AS DOUBLE), 2) as views_per_user
FROM product_popularity
ORDER BY view_count DESC
LIMIT 10;
```

#### Query 4: Category Performance

**Business Question:** How do product categories perform day-over-day?

```sql
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
ORDER BY event_date DESC, total_revenue DESC
LIMIT 20;
```

#### Query 5: User Activity Trends

**Business Question:** How engaged are users day-to-day?

```sql
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
```

> **Complete SQL:** All queries with detailed comments are available in `queries.sql`

### 10\. Verify Incremental Processing

To test job bookmarks:

1.  Run the ETL job again **without** generating new data:
    ```bash
    JOB_RUN_ID=$(aws glue start-job-run \
      --job-name capstone-etl-job-<student-id> \
      --region us-west-2 \
      --query 'JobRunId' \
      --output text)
    
    # Wait 90 seconds, then check
    aws glue get-job-run \
      --job-name capstone-etl-job-<student-id> \
      --run-id $JOB_RUN_ID \
      --region us-west-2 \
      --query 'JobRun.[JobRunState,ExecutionTime]'
    ```

2.  **Expected Result:** Job completes in ~45-60 seconds (vs 3-20 minutes for first run)
3.  Generate new data (Step 4) and re-run to process only the delta.

-----

## Key Design Decisions

### Incremental Processing

  * **Implementation:** Glue job bookmarks track processed files at the partition level.
  * **Benefit:** drastically reduces cost and time by avoiding reprocessing historical data.
  * **Verification:** Re-running ETL on empty delta takes \~59s vs \~20 mins.

### Tool Selection

  * **AWS Glue:** Serverless ETL eliminates cluster management overhead.
  * **Athena:** Pay-per-query pricing ($5/TB scanned) is ideal for ad-hoc analytics.
  * **Parquet Format:** Columnar storage enables predicate pushdown and column pruning.

### Pre-Aggregation Strategy

  * **Why:** Reduces query latency from **10.9s to 0.66s** which is about 16.6x speedup.
  * **Cost Impact:** Scans **412,027x less data** on average.
  * **Trade-off:** Increases ETL complexity in exchange for massive query performance gains.

### Single PySpark Script

  * **Atomic Execution:** Ensures all three layers succeed or fail together, preventing data inconsistency.
  * **Version Control:** Full transformation logic resides in Git rather than GUI configurations.
  * **Capabilities:** Allows for complex logic for processing timestamps and other transformations that is difficult in visual ETL tools.

-----

## Performance Benchmarks

| Metric | Bronze Queries | Gold Queries | Improvement |
| :--- | :--- | :--- | :--- |
| **Avg Query Time** | 10.9s | 0.66s | **16.6x faster** |
| **Avg Data Scanned** | 419.69 MB | 2.0 KB | **412,027x less** |
| **Storage (25.9M events)** | 440 MB | 29 KB | **14,779x reduction** |

-----

## Troubleshooting

**ETL Job Fails**
Check the error message:

```bash
aws glue get-job-runs --job-name capstone-etl-job-<student-id> \
  --max-items 1 --query 'JobRuns[0].ErrorMessage'
```

**No Gold Tables After ETL**
Verify the ETL script actually wrote data to S3:

```bash
aws s3 ls s3://${BUCKET}/capstone/gold/ --recursive
```

**Query Returns No Results**
Check if the specific table partition is populated:

```bash
aws s3 ls s3://${BUCKET}/capstone/gold/product_funnel/ --recursive
```

-----

## Cleanup

To avoid ongoing charges, delete the CloudFormation stack. The S3 bucket will be auto-emptied via a custom resource.

```bash
aws cloudformation delete-stack --stack-name capstone-pipeline
```

-----

## Notes

  * **Estimated Cost:** $1.50-$2.00 for my full test run that processed 25.9M events.
  * **Schedules:** EventBridge schedules are deployed as `DISABLED` to prevent accidental cost overruns.
  * **Manual Trigger:** The ETL job requires a manual trigger as LabRole permissions prevent automatic EventBridge to Glue triggers.
  * **Important:** Remember to upload the `etl_script.py` to the S3 bucket before running the ETL job.
  * **Console Option:** You can deploy the stack and run all operations via the AWS Console instead of CLI.