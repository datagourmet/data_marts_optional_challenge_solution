"""
Test Challenge 06 — Checkpoint 1: Customer Dimension

Validates that students have created:
- int_customer_order_summary.sql in models/intermediate/
  - Uses {{ ref('int_orders_with_payments') }}
  - Aggregates with GROUP BY and uses MIN, MAX, COUNT, SUM
- dim_customers.sql in models/marts/
  - Uses {{ ref('stg_customers') }} and {{ ref('int_customer_order_summary') }}
  - Uses LEFT JOIN to preserve all customers
  - Materialized as 'table'

Run: pytest tests/test_dim_customers.py -v
"""

import pytest
from pathlib import Path


@pytest.fixture
def project_dir():
    """Get jaffle_shop_dbt directory within challenge repo."""
    challenge_dir = Path(__file__).parent.parent
    dbt_project_dir = challenge_dir / "jaffle_shop_dbt"
    assert dbt_project_dir.exists(), (
        f"❌ jaffle_shop_dbt/ directory not found in {challenge_dir}\n"
        f"   Did you copy your dbt project from the previous challenge? (Section 0)\n"
        f"   Run: ls .. to find the previous challenge directory, then:\n"
        f"   cp -rP ../PREVIOUS-CHALLENGE/jaffle_shop_dbt ."
    )
    return dbt_project_dir


@pytest.fixture
def marts_dir(project_dir):
    """Get the models/marts directory."""
    marts = project_dir / "models" / "marts"
    assert marts.exists(), (
        "❌ models/marts/ directory not found\n"
        "   Did you create the marts folder? (Section 1)\n"
        "   Run: mkdir -p models/marts"
    )
    return marts


@pytest.fixture
def intermediate_dir(project_dir):
    """Get the models/intermediate directory."""
    intermediate = project_dir / "models" / "intermediate"
    assert intermediate.exists(), (
        "❌ models/intermediate/ directory not found\n"
        "   Did you copy your dbt project from the previous challenge? (Section 0)"
    )
    return intermediate


class TestIntCustomerOrderSummary:
    """Checkpoint 1a: Validate int_customer_order_summary.sql was created correctly."""

    @pytest.fixture
    def summary_path(self, intermediate_dir):
        """Path to int_customer_order_summary.sql."""
        path = intermediate_dir / "int_customer_order_summary.sql"
        assert path.exists(), (
            "❌ models/intermediate/int_customer_order_summary.sql not found\n"
            "   Did you create the customer order summary intermediate model? (Section 2.2)\n"
            "   Run: code models/intermediate/int_customer_order_summary.sql"
        )
        return path

    @pytest.fixture
    def summary_content(self, summary_path):
        """Content of int_customer_order_summary.sql."""
        with open(summary_path, 'r') as f:
            return f.read()

    def test_summary_exists(self, summary_path):
        """Must create models/intermediate/int_customer_order_summary.sql."""
        assert summary_path.exists()

    def test_summary_references_int_orders(self, summary_content):
        """int_customer_order_summary must reference int_orders_with_payments."""
        assert "int_orders_with_payments" in summary_content, (
            "❌ int_customer_order_summary.sql must reference int_orders_with_payments\n"
            "   Did you use {{ ref('int_orders_with_payments') }}?"
        )

    def test_summary_has_aggregation(self, summary_content):
        """int_customer_order_summary must GROUP BY customer_id."""
        assert "GROUP BY" in summary_content.upper(), (
            "❌ int_customer_order_summary.sql must aggregate with GROUP BY\n"
            "   Did you GROUP BY customer_id to produce one row per customer?"
        )

    def test_summary_has_aggregate_functions(self, summary_content):
        """int_customer_order_summary must use MIN, MAX, COUNT, SUM."""
        content_upper = summary_content.upper()
        has_count = "COUNT(" in content_upper
        has_sum = "SUM(" in content_upper
        has_min = "MIN(" in content_upper
        has_max = "MAX(" in content_upper

        assert has_count and has_sum, (
            "❌ int_customer_order_summary.sql must use COUNT() and SUM()\n"
            "   Did you calculate number_of_orders with COUNT() and lifetime_value with SUM()?"
        )
        assert has_min and has_max, (
            "❌ int_customer_order_summary.sql must use MIN() and MAX() for order dates\n"
            "   Did you calculate first_order_date with MIN(order_date) and "
            "most_recent_order_date with MAX(order_date)?"
        )


class TestDimCustomers:
    """Checkpoint 1b: Validate dim_customers.sql was created correctly."""

    @pytest.fixture
    def dim_customers_path(self, marts_dir):
        """Path to dim_customers.sql."""
        path = marts_dir / "dim_customers.sql"
        assert path.exists(), (
            "❌ models/marts/dim_customers.sql not found\n"
            "   Did you create dim_customers.sql? (Section 3.2)\n"
            "   Run: code models/marts/dim_customers.sql"
        )
        return path

    @pytest.fixture
    def dim_customers_content(self, dim_customers_path):
        """Content of dim_customers.sql."""
        with open(dim_customers_path, 'r') as f:
            return f.read()

    def test_dim_customers_exists(self, dim_customers_path):
        """Must create models/marts/dim_customers.sql."""
        assert dim_customers_path.exists()

    def test_dim_customers_uses_ref(self, dim_customers_content):
        """dim_customers must use {{ ref() }} to reference source models."""
        assert "{{ ref(" in dim_customers_content, (
            "❌ dim_customers.sql must use {{ ref() }} function\n"
            "   Did you reference stg_customers and int_customer_order_summary with ref()?\n"
            "   Example: SELECT * FROM {{ ref('stg_customers') }}"
        )

    def test_dim_customers_references_stg_customers(self, dim_customers_content):
        """dim_customers must reference stg_customers for customer details."""
        assert "stg_customers" in dim_customers_content, (
            "❌ dim_customers.sql must reference stg_customers\n"
            "   Did you pull customer details from {{ ref('stg_customers') }}?"
        )

    def test_dim_customers_references_int_customer_order_summary(self, dim_customers_content):
        """dim_customers must reference int_customer_order_summary for pre-aggregated metrics."""
        assert "int_customer_order_summary" in dim_customers_content, (
            "❌ dim_customers.sql must reference int_customer_order_summary\n"
            "   Did you pull pre-aggregated order metrics from {{ ref('int_customer_order_summary') }}?\n"
            "   The aggregation logic belongs in the intermediate layer, not in the mart."
        )

    def test_dim_customers_has_left_join(self, dim_customers_content):
        """dim_customers must use LEFT JOIN to preserve all customers."""
        content_upper = dim_customers_content.upper()
        assert "LEFT JOIN" in content_upper or "LEFT\nJOIN" in content_upper, (
            "❌ dim_customers.sql must use LEFT JOIN to keep customers without orders\n"
            "   Did you join customer_metrics back to customers with LEFT JOIN?\n"
            "   An INNER JOIN would drop customers who have never placed an order!"
        )

    def test_dim_customers_materialized_as_table(self, dim_customers_content):
        """dim_customers must be materialized as table (not view) for performance."""
        assert (
            "materialized='table'" in dim_customers_content
            or 'materialized="table"' in dim_customers_content
        ), (
            "❌ dim_customers.sql must have materialized='table' in config block\n"
            "   Did you add {{ config(materialized='table', schema='marts') }} at the top?\n"
            "   Marts are tables, not views, for dashboard performance."
        )
