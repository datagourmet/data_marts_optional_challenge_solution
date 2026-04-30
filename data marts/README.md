## Context

In the previous challenge, you built `int_orders_with_payments` — joining orders with payments and aggregating totals to the order level. You now have a clean intermediate layer ready for consumption. The mart layer is the final step: analytics-ready tables designed for business consumers and dashboards.

This challenge extends your dbt project to the full three-layer architecture (staging → intermediate → marts). Complete it if you have time — it gives you a working end-to-end pipeline and demonstrates the full dbt data model. The skills covered here are built on further in the next unit.

## Objective

Create two mart models:

- `dim_customers.sql` - Customer dimension with aggregated metrics
- `fct_orders.sql` - Order fact table ready for analysis

## 0. Set Up Challenge Environment

**📍 In your terminal**, navigate to the challenge directory and copy your previous work:

```bash
cp -rP ../../../{{ local_path_to("03-Data-Transformation/09-Data-Layers-And-Intro-DBT/05-Intermediate-Layer") }}/jaffle_shop_dbt .

```

Then commit so you have a clean starting point for this challenge:

```bash
git add jaffle_shop_dbt
git commit -m "Copied setup from previous challenge"
git push origin master
```

**🔧 Troubleshooting:**

<details>
<summary markdown="span">**Symlink not working?**</summary>

**If symlink is broken:**

```bash
rm -f jaffle_shop_dbt/dev.duckdb
ln -s ../../../dbt-shared/dev.duckdb jaffle_shop_dbt/dev.duckdb
```

**DBeaver connection:**

- Keep using your existing `jaffle_shop_shared` connection
- No path updates needed!

</details>

## 1. Create Mart Folder

**📍 In your terminal:**

```bash
mkdir -p jaffle_shop_dbt/models/marts
```

## 2. Pre-Aggregate Customer Metrics

### 2.1. Why Not Just GROUP BY in the Mart?

You could write `dim_customers` as a single model that aggregates order data and joins it to customers all in one place. It would work. But take a look at what that mart would contain:

```sql
-- ⚠️ Possible but not ideal: aggregation inside a mart
WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
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
),

final AS (
    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_metrics.first_order_date,
        ...
    FROM customers
    LEFT JOIN customer_metrics ON ...
)
```

That `GROUP BY customer_id` inside `customer_metrics` is business logic — it calculates per-customer order summaries. That belongs in the **intermediate layer**, not the mart. The mart should just assemble the final output from pre-built blocks.

**Why does this matter?**

- **Reusability:** If another mart (e.g. a churn model) also needs per-customer order counts, it can reference `int_customer_order_summary` directly — no duplication
- **Testability:** You can test intermediate models independently; logic buried in a mart is harder to isolate
- **Readability:** Mart models should be easy to read at a glance — a mart that does aggregation, joins, and config all at once is doing too much
- **Separation of concerns:** Intermediate = "prepare the data"; Mart = "assemble the final shape"

The fix is one extra model in the intermediate layer that handles the aggregation. The mart then becomes a simple join.

### 2.2. Create int_customer_order_summary.sql

**Your Challenge:** Add one more intermediate model that summarises each customer's order history — one row per customer.

**Requirements:**

1. **File location:** `models/intermediate/int_customer_order_summary.sql`
2. **Reference** `int_orders_with_payments` using `{{ ref() }}`
3. **Group by** `customer_id` — one output row per customer
4. **Output columns:** `customer_id`, `first_order_date`, `most_recent_order_date`, `number_of_orders`, `lifetime_value`

**📍 In your terminal**, navigate to your dbt project if you are not already there:

```bash
cd jaffle_shop_dbt
```

Then create the file:

```bash
code models/intermediate/int_customer_order_summary.sql
```

**📝 In VS Code**, write your SQL.

<details>
<summary markdown="span">**💡 Hint: CTE structure template**</summary>

```sql
WITH orders AS (
    SELECT * FROM {{ ref('???') }}
),

customer_metrics AS (
    SELECT
        ???,
        ???(order_date)    AS first_order_date,
        ???(order_date)    AS most_recent_order_date,
        ???(order_id)      AS number_of_orders,
        ???(total_amount)  AS lifetime_value
    FROM orders
    GROUP BY ???
)

SELECT * FROM customer_metrics
```

Fill in all `???` based on the requirements above.

</details>

<details>
<summary markdown="span">**💡 Hint: SQL aggregation functions**</summary>

- `first_order_date` → `MIN(order_date)` — earliest order date
- `most_recent_order_date` → `MAX(order_date)` — latest order date
- `number_of_orders` → `COUNT(order_id)` — how many orders
- `lifetime_value` → `SUM(total_amount)` — total amount spent

**🗄️ In DBeaver**, check what columns `int_orders_with_payments` has:

```sql
SELECT * FROM main_intermediate.int_orders_with_payments LIMIT 5;
```

</details>

## 3. Build Customer Dimension

### 3.1. Why Tables for Marts?

**Marts are different from staging and intermediate models — they use `materialized='table'`.**

- **Staging and intermediate** use `view` — lightweight, always fresh, cheap to maintain
- **Marts** use `table` — pre-computed, fast to query, great for dashboards

Marts are queried heavily by stakeholders and BI tools. Without pre-computing, every dashboard query would re-run all your aggregations. Tables store the result so queries are instant.

You'll configure this in the `{{ config() }}` block at the top of each mart model.

### 3.2. Create dim_customers.sql

**Your Challenge:** Create a customer dimension table by joining customer details to the pre-aggregated order summary you just built.

**Requirements:**

1. **File location:** `models/marts/dim_customers.sql`
2. **Add a `{{ config() }}` block** at the top — materialize as `table` in the `marts` schema
3. **Reference** `stg_customers` (customer details) and `int_customer_order_summary` (pre-aggregated order metrics) using `{{ ref() }}`
4. **Join** the two models — preserve **all** customers (even those with no orders)
5. **Output columns:** `customer_id`, `first_name`, `last_name`, `first_order_date`, `most_recent_order_date`, `number_of_orders`, `lifetime_value`

**Create the file:**

```bash
code models/marts/dim_customers.sql
```

**📝 In VS Code**, write your SQL using the CTE pattern.

<details>
<summary markdown="span">**💡 Hint: CTE structure template**</summary>

```sql
{{ config(
    materialized='???',
    schema='???'
) }}

WITH customers AS (
    SELECT * FROM {{ ref('???') }}
),

customer_metrics AS (
    SELECT * FROM {{ ref('???') }}
),

final AS (
    SELECT
        customers.customer_id,
        customers.???,
        customers.???,
        customer_metrics.first_order_date,
        customer_metrics.most_recent_order_date,
        customer_metrics.number_of_orders,
        customer_metrics.lifetime_value
    FROM customers
    ??? JOIN customer_metrics
        ON customers.??? = customer_metrics.???
)

SELECT * FROM final
```

The aggregation logic is already handled by `int_customer_order_summary` — this model just joins the pre-built blocks together.

</details>

### 3.3. Run dim_customers

**🗄️ In DBeaver**, disconnect from your database:

1. **Right-click your `jaffle_shop_shared` connection** in the Database Navigator
2. **Select "Disconnect"**

**📍 In your terminal**, navigate to your dbt project if you are not already there:

```bash
cd jaffle_shop_dbt
```

Then run the dimension model:

```bash
dbt run --select dim_customers
```

<details>
<summary markdown="span">**Expected output**</summary>

```bash
Completed successfully
Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

</details>

**Verify your customer dimension:**

**🗄️ In DBeaver**, connect to `jaffle_shop_shared` and run:

```sql
-- Check all output columns exist
SELECT customer_id, first_name, last_name,
       first_order_date, most_recent_order_date,
       number_of_orders, lifetime_value
FROM main_marts.dim_customers
LIMIT 10;
```

**Verify:**

- All 7 columns appear
- `lifetime_value` is NULL for customers with no orders (correct — LEFT JOIN!)
- `number_of_orders` shows whole numbers like 1, 2, 3

```sql
-- Verify row count matches stg_customers (LEFT JOIN keeps all customers)
SELECT COUNT(*) FROM main_marts.dim_customers;
-- Compare with:
SELECT COUNT(*) FROM main_staging.stg_customers;
-- These should be equal
```

**❌ Common mistakes:**

- Fewer rows than `stg_customers`? You used INNER JOIN — switch to LEFT JOIN!
- `lifetime_value` shows 0 instead of NULL for no-order customers? Check your JOIN logic.
- Missing metric columns? Make sure `int_customer_order_summary` is referenced correctly with `{{ ref() }}`.

### 🧪 Checkpoint 1: Push Customer Dimension

**Before pushing, validate your dimension model:**

**📍 In your terminal**, if you are not already in the challenge directory, navigate there now:

```bash
cd ..
```

Then run the checkpoint 1 tests:

```bash
pytest tests/test_dim_customers.py -v
```

**If tests pass**, commit your dimension model:

```bash
# Stage both the new intermediate model and the marts directory
git add jaffle_shop_dbt/models/intermediate/int_customer_order_summary.sql
git add jaffle_shop_dbt/models/marts/

# Commit with descriptive message
git commit -m "Add int_customer_order_summary and dim_customers"

# Push to GitHub (triggers automated tests)
git push origin master
```

`dim_customers` is running as a physical table — one row per customer, assembled from pre-built intermediate blocks. Now you'll build the companion fact table that records each individual order.

## 4. Build Order Fact Table

### 4.1. Create fct_orders.sql

**Your Challenge:** Create an order fact table that enriches each order with customer name information from the staging layer.

**Why join to `stg_customers` directly instead of `dim_customers`?** It is tempting to reference `dim_customers` here since it already has the customer attributes you need — but this creates a mart-to-mart dependency (one mart model depending on another). This coupling makes both models harder to evolve independently. If you change `dim_customers`, you risk breaking `fct_orders` unexpectedly. The better pattern: fact tables join to the **staging layer** for attribute lookups, not to other mart models. The trade-off is slightly less denormalization — `fct_orders` won't include aggregated customer metrics like `number_of_orders`, which live in `dim_customers` where they belong.

**Requirements:**

1. **File location:** `models/marts/fct_orders.sql`
2. **Add a `{{ config() }}` block** — materialize as `table` in the `marts` schema
3. **Reference** `int_orders_with_payments` (order data) and `stg_customers` (customer name) using `{{ ref() }}`
4. **Join** orders with customers — preserve **all** orders
5. **Output columns:** `order_id`, `customer_id`, `first_name`, `last_name`, `order_date`, `status`, `total_amount`

**Create the file:**

```bash
code models/marts/fct_orders.sql
```

**📝 In VS Code**, write your SQL using the CTE pattern.

<details>
<summary markdown="span">**💡 Hint: CTE structure template**</summary>

```sql
{{ config(
    materialized='???',
    schema='???'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('???') }}
),

customers AS (
    SELECT * FROM {{ ref('???') }}
),

final AS (
    SELECT
        orders.order_id,
        orders.customer_id,
        customers.???,
        customers.???,
        orders.order_date,
        orders.???,
        orders.total_amount
    FROM orders
    ??? JOIN customers
        ON orders.??? = customers.???
)

SELECT * FROM final
```

Fill in all `???` based on the requirements above.

</details>

<details>
<summary markdown="span">**💡 Hint: Why join to stg_customers instead of dim_customers?**</summary>

`fct_orders` needs customer names (`first_name`, `last_name`) to enrich order records. These come from `stg_customers`, which is available in the staging layer.

Referencing `dim_customers` would work — but it creates a dependency between two mart models. When you run `dbt run --select fct_orders`, dbt would need to build `dim_customers` first. Worse, if you ever change `dim_customers`, you have to verify nothing in `fct_orders` breaks.

Staging models are stable raw-data references; intermediate models are pre-joined building blocks. Marts should pull from whichever layer is most appropriate — staging for raw attributes, intermediate for pre-aggregated data.

```plaintext
stg_customers → fct_orders   ✓ (staging → mart: raw attribute lookup)
stg_customers → dim_customers → fct_orders   ✗ (mart → mart: avoidable coupling)
```

</details>

### 4.2. Run fct_orders

**📍 In your terminal**, run the fact model:

```bash
# Navigate to dbt project (if not already there)
cd jaffle_shop_dbt
```

Then run the fact model:

```bash
dbt run --select fct_orders
```

<details>
<summary markdown="span">**Expected output**</summary>

```bash
Completed successfully
Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

</details>

**Verify your order fact table:**

**🗄️ In DBeaver**, connect to `jaffle_shop_shared` and run:

```sql
-- Check all output columns exist
SELECT order_id, customer_id, first_name, last_name,
       order_date, status, total_amount
FROM main_marts.fct_orders
LIMIT 10;
```

**Verify:**

- All 7 columns appear
- `first_name` and `last_name` are populated (JOIN worked)

```sql
-- Verify row count matches int_orders_with_payments (LEFT JOIN keeps all orders)
SELECT COUNT(*) FROM main_marts.fct_orders;
-- Compare with:
SELECT COUNT(*) FROM main_intermediate.int_orders_with_payments;
-- These should be equal
```

**❌ Common mistakes:**

- `first_name` is NULL? Your JOIN is not matching on the right column.
- Fewer rows than `int_orders_with_payments`? You used INNER JOIN — switch to LEFT JOIN!

### 🧪 Checkpoint 2: Push Order Fact Table

**Before pushing, validate both mart models:**

**📍 In your terminal**, if you are not already in the challenge directory, navigate there now:

```bash
cd ..
```

Then run the checkpoint 2 tests:

```bash
pytest tests/test_fct_orders.py -v
```

**If tests pass**, commit your fact model:

```bash
# Stage the fact model
git add jaffle_shop_dbt/models/marts/fct_orders.sql

# Commit with descriptive message
git commit -m "Add fct_orders mart model"

# Push to GitHub (triggers automated tests)
git push origin master
```

Both mart models are in place. The final step is to document them in `schema.yml` — descriptions for the models and every column — so the pipeline is self-describing.

## 5. Run and Verify

### 5.1. Build Both Marts

```bash
dbt run --select marts
```

<details>
<summary markdown="span">**Expected output**</summary>

```bash
Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

</details>

### 5.2. Query Results

**🗄️ In DBeaver**, reconnect to your `jaffle_shop_shared` connection and query the results:

```sql
-- Check customer dimension
SELECT * FROM main_marts.dim_customers
ORDER BY lifetime_value DESC
LIMIT 10;

-- Identify VIP customers
SELECT
    customer_id,
    first_name,
    last_name,
    number_of_orders,
    lifetime_value,
    ROUND(lifetime_value / number_of_orders, 2) AS avg_order_value
FROM main_marts.dim_customers
WHERE number_of_orders > 5
ORDER BY lifetime_value DESC;

-- Check fact table
SELECT * FROM main_marts.fct_orders
LIMIT 10;

-- Orders by high-value customers
SELECT
    order_id,
    first_name || ' ' || last_name AS customer_name,
    order_date,
    total_amount
FROM main_marts.fct_orders
ORDER BY order_date DESC
LIMIT 20;
```

### 5.3. View Complete Lineage

```bash
dbt docs generate && dbt docs serve
```

Lineage should show:

```markdown
jaffle_shop.customers → stg_customers  ────────────🯓────────────────────────🯓
                                                    🯒🯓                       🯒🯓
                                                      🯟→ fct_orders          dim_customers
 jaffle_shop.orders   →  stg_orders                 🯐🯑                       🯐🯑
                              🯟→ int_orders_with_payments → int_customer_order_summary
jaffle_shop.payments  → stg_payments
```

## 6. Configure Mart Layer

### 6.1. Update dbt_project.yml

Add the following `marts` section to your `dbt_project.yml`. Here is the complete `models:` config with all three layers — copy it in full:

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

> **Note:** Marts use `materialized: table` instead of `view`. Tables are pre-computed and stored, which makes dashboard queries faster — marts are the final consumer-facing layer.

### 6.2. Rebuild with Config

```bash
dbt run --select marts
```

## 7. Document the Marts

**Your task:** Create `models/marts/schema.yml` and document both mart models.

**Requirements:**

1. **Both models** (`dim_customers` and `fct_orders`) must be documented
2. **Each model** must have a meaningful description (purpose and grain)
3. **Each column** should have a description

**Create the file:**

```bash
code models/marts/schema.yml
```

<details>
<summary markdown="span">**💡 Hint: schema.yml structure**</summary>

Follow the same pattern as `models/staging/schema.yml` from the staging layer:

```yaml
version: 2

models:
  - name: dim_customers
    description: "..."
    columns:
      - name: customer_id
        description: "..."
      # all 7 output columns

  - name: fct_orders
    description: "..."
    columns:
      # all 7 output columns
```

</details>

<details>
<summary markdown="span">**🔍 Reminder: output columns for each model**</summary>

**dim_customers:** `customer_id`, `first_name`, `last_name`, `first_order_date`, `most_recent_order_date`, `number_of_orders`, `lifetime_value`

**fct_orders:** `order_id`, `customer_id`, `first_name`, `last_name`, `order_date`, `status`, `total_amount`

</details>

### 🧪 Checkpoint 3: Push Documentation

**Before pushing, validate your documentation:**

**📍 In your terminal**, if you are not already in the challenge directory, navigate there now:

```bash
cd ..
```

Then run the checkpoint 3 tests:

```bash
pytest tests/test_all_marts.py -v
```

**Optional: Run all tests together:**

```bash
make
```

**If tests pass**, commit your documentation:

```bash
# Stage the schema file
git add jaffle_shop_dbt/models/marts/schema.yml

# Commit with descriptive message
git commit -m "Add documentation for mart models"

# Push to GitHub (triggers automated tests)
git push origin master
```

The full four-layer pipeline is complete and documented. In the next challenge you'll apply everything you've learned independently to a new dataset.

<details>
<summary markdown="span">**📚 Quick Reference**</summary>

```bash
# Build pipeline
dbt run                    # All models
dbt run --select marts        # Just marts
dbt run --full-refresh     # Rebuild all tables from scratch

# Query results (in DBeaver - jaffle_shop_shared connection)
SELECT * FROM main_marts.dim_customers;
SELECT * FROM main_marts.fct_orders;
```

### Your Complete Pipeline

```text
RAW LAYER (Sources)
└── Raw data from production systems (Jaffle Shop)

STAGING LAYER (Views)
└── Clean, typed, 1:1 with sources

INTERMEDIATE LAYER (Views)
└── Joins, calculations, business logic

MART LAYER (Tables)
└── Aggregated, stakeholder-ready
```

### Granularity Evolution

```text
stg_orders: order level
stg_payments: payment level
↓
int_orders_with_payments: order level (aggregated payments)
↓
int_customer_order_summary: customer level (aggregated orders)
↓
dim_customers: customer level (joined from stg_customers + int_customer_order_summary)
fct_orders: order level (enriched with customer data)
```

</details>

---

## 🎉 Challenge Complete

You have a complete 4-layer pipeline: raw → staging → intermediate → marts.

**Key takeaways:**

- Fact tables record events; dimension tables describe entities — marts join them for stakeholder consumption
- Materialise mart models as tables, not views — dashboards need consistent query performance
- Move aggregation logic into the intermediate layer so mart models stay simple joins
- Intermediate models are reusable building blocks — multiple marts can reference the same pre-aggregated model
