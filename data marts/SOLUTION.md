# Solution

> **Builds on:** [05-Intermediate-Layer]({{ local_path_to("03-Data-Transformation/09-Data-Layers-And-Intro-DBT/05-Intermediate-Layer") }}) — copy the `jaffle_shop_dbt/` solution files from that challenge as your starting point before reviewing this one.

## Step 1: Create Marts Directory

```bash
mkdir -p models/marts
```

## Step 2: Create Customer Order Summary (Intermediate)

Create `models/intermediate/int_customer_order_summary.sql`:

```sql
WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_payments') }}
),

customer_metrics AS (
    SELECT
        customer_id,
        MIN(order_date)   AS first_order_date,
        MAX(order_date)   AS most_recent_order_date,
        COUNT(order_id)   AS number_of_orders,
        SUM(total_amount) AS lifetime_value
    FROM orders
    GROUP BY customer_id
)

SELECT * FROM customer_metrics
```

**Why a separate intermediate model?** Moving the `GROUP BY` out of the mart keeps `dim_customers` as a simple join. The aggregation is now reusable — any other mart can reference `int_customer_order_summary` without duplicating logic. Marts should assemble pre-built blocks, not perform their own aggregations.

## Step 3: Create Customer Dimension

```sql
{{ config(
    materialized='table',
    schema='marts'
) }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_metrics AS (
    SELECT * FROM {{ ref('int_customer_order_summary') }}
),

final AS (
    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_metrics.first_order_date,
        customer_metrics.most_recent_order_date,
        customer_metrics.number_of_orders,
        customer_metrics.lifetime_value
    FROM customers
    LEFT JOIN customer_metrics
        ON customers.customer_id = customer_metrics.customer_id
)

SELECT * FROM final
```

## Step 4: Run Customer Dimension

```bash
cd jaffle_shop_dbt
```

Then run both models (the new intermediate first, then the mart):

```bash
dbt run --select int_customer_order_summary dim_customers
```

## Step 5: Create Order Fact Table

Create `models/marts/fct_orders.sql`:

```sql
{{ config(
    materialized='table',
    schema='marts'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_payments') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT
        orders.order_id,
        orders.customer_id,
        customers.first_name,
        customers.last_name,
        orders.order_date,
        orders.status,
        orders.total_amount
    FROM orders
    LEFT JOIN customers
        ON orders.customer_id = customers.customer_id
)

SELECT * FROM final
```

**Why join to `stg_customers` instead of `dim_customers`?** Referencing `dim_customers` would create a mart-to-mart dependency — one mart model depending on another in the same layer. This coupling makes both models harder to evolve independently. The better pattern: fact tables join to the **staging layer** for attribute lookups. The trade-off is that `fct_orders` won’t include aggregated customer metrics (like `number_of_orders` or `lifetime_value`); those belong in `dim_customers` where they are computed once, at the right grain.

## Step 6: Run Order Fact Table

```bash
dbt run --select fct_orders
```

## Step 7: Run All Marts

```bash
dbt run --select marts
```

Expected output:

```bash
Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

## Step 8: Configure dbt_project.yml

Update `jaffle_shop_dbt/dbt_project.yml` to add the `marts:` section under `models:`. This sets the default materialization and schema for all mart models — overriding the individual `{{ config() }}` blocks is optional but ensures consistency across the layer:

```yaml
models:
  jaffle_shop_dbt:
    +materialized: view
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts
```

Rebuild the mart layer to apply the config:

```bash
dbt run --select marts
```

Expected output:

```bash
Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

## Step 9: Document the Marts

```yaml
version: 2

models:
  - name: dim_customers
    description: |
      Customer dimension with aggregated lifetime metrics.
      Materialized as table for dashboard performance.
    columns:
      - name: customer_id
        description: "Unique customer identifier"

      - name: first_name
        description: "Customer first name"

      - name: last_name
        description: "Customer last name"

      - name: first_order_date
        description: "Date of customer's first order"

      - name: most_recent_order_date
        description: "Date of customer's most recent order"

      - name: number_of_orders
        description: "Total number of orders placed by customer"

      - name: lifetime_value
        description: "Total amount paid by customer across all orders"

  - name: fct_orders
    description: |
      Order fact table enriched with customer name attributes.
      Grain: one row per order. References stg_customers directly
      to avoid a mart-to-mart dependency on dim_customers.
    columns:
      - name: order_id
        description: "Unique order identifier"

      - name: customer_id
        description: "Customer who placed the order"

      - name: first_name
        description: "Customer first name (denormalized from stg_customers)"

      - name: last_name
        description: "Customer last name (denormalized from stg_customers)"

      - name: order_date
        description: "Date order was placed"

      - name: status
        description: "Order status"

      - name: total_amount
        description: "Total payment amount for this order"
```

## Step 10: Verify Results

Query the marts in DBeaver:

```sql
-- Check customer dimension
SELECT * FROM main_marts.dim_customers
ORDER BY lifetime_value DESC
LIMIT 10;

-- Check fact table
SELECT * FROM main_marts.fct_orders
LIMIT 10;

-- Orders with customer names
SELECT
    order_id,
    first_name || ' ' || last_name AS customer_name,
    order_date,
    total_amount
FROM main_marts.fct_orders
ORDER BY order_date DESC;
```

## Troubleshooting

**"String concatenation with NULL returns NULL"**

- Use `COALESCE()`: `COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')`
- Or handle NULLs in staging layer

**Should this be fct_ or dim_?**

- **fct_ (Facts)**: Transactions, events, measurements (orders, payments, page views)
- **dim_ (Dimensions)**: Entities, attributes, reference data (customers, products, dates)
- Rule of thumb: If it has a timestamp/date → probably a fact. If it describes "who/what" → probably a dimension.

## Naming Convention

Marts MUST start with either:

- `fct_` for fact tables (transactional data)
- `dim_` for dimension tables (reference data)

Examples:

- ✅ `fct_orders.sql`
- ✅ `fct_daily_sales.sql`
- ✅ `dim_customers.sql`
- ✅ `dim_products.sql`
- ❌ `orders.sql` (no prefix)
- ❌ `mart_orders.sql` (wrong prefix)

## Materialization

Marts should be materialized as tables for performance:

```sql
{{ config(materialized='table') }}

SELECT ...
```

**Why tables for marts?**

- Stakeholders query frequently
- Complex aggregations expensive to recompute
- Fast query performance matters for dashboards
- Data refreshes on schedule (dbt run)
