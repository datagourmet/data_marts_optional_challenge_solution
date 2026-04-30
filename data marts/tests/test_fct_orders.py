"""
Test Challenge 06 — Checkpoint 2: Order Fact Table

Validates that students have created fct_orders.sql correctly:
- File exists at models/marts/fct_orders.sql
- Uses {{ ref() }} to reference int_orders_with_payments and stg_customers
- JOINs with stg_customers to add first_name and last_name
- Uses fct_ naming prefix
- Materialized as 'table'

Run: pytest tests/test_fct_orders.py -v
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


class TestFctOrders:
    """Checkpoint 2: Validate fct_orders.sql was created correctly."""

    @pytest.fixture
    def fct_orders_path(self, marts_dir):
        """Path to fct_orders.sql."""
        path = marts_dir / "fct_orders.sql"
        assert path.exists(), (
            "❌ models/marts/fct_orders.sql not found\n"
            "   Did you create fct_orders.sql? (Section 4.1)\n"
            "   Run: code models/marts/fct_orders.sql"
        )
        return path

    @pytest.fixture
    def fct_orders_content(self, fct_orders_path):
        """Content of fct_orders.sql."""
        with open(fct_orders_path, 'r') as f:
            return f.read()

    def test_fct_orders_exists(self, fct_orders_path):
        """Must create models/marts/fct_orders.sql."""
        assert fct_orders_path.exists()

    def test_fct_orders_uses_ref(self, fct_orders_content):
        """fct_orders must use {{ ref() }} to reference source models."""
        assert "{{ ref(" in fct_orders_content, (
            "❌ fct_orders.sql must use {{ ref() }} function\n"
            "   Did you reference int_orders_with_payments and stg_customers with ref()?\n"
            "   Example: SELECT * FROM {{ ref('int_orders_with_payments') }}"
        )

    def test_fct_orders_references_int_orders(self, fct_orders_content):
        """fct_orders must reference int_orders_with_payments for order data."""
        assert "int_orders_with_payments" in fct_orders_content, (
            "❌ fct_orders.sql must reference int_orders_with_payments\n"
            "   Did you pull order data from {{ ref('int_orders_with_payments') }}?"
        )

    def test_fct_orders_references_stg_customers(self, fct_orders_content):
        """fct_orders must reference stg_customers directly, not dim_customers."""
        assert "stg_customers" in fct_orders_content, (
            "❌ fct_orders.sql must reference stg_customers\n"
            "   Did you join with {{ ref('stg_customers') }} to add customer name attributes?\n"
            "   Tip: reference stg_customers directly to avoid a mart-to-mart dependency."
        )

    def test_fct_orders_has_join(self, fct_orders_content):
        """fct_orders must JOIN stg_customers to add customer name attributes."""
        content_upper = fct_orders_content.upper()
        assert "JOIN" in content_upper, (
            "❌ fct_orders.sql must JOIN orders with stg_customers\n"
            "   Did you write a LEFT JOIN on customer_id?"
        )

    def test_fct_orders_materialized_as_table(self, fct_orders_content):
        """fct_orders must be materialized as table (not view) for performance."""
        assert (
            "materialized='table'" in fct_orders_content
            or 'materialized="table"' in fct_orders_content
        ), (
            "❌ fct_orders.sql must have materialized='table' in config block\n"
            "   Did you add {{ config(materialized='table', schema='marts') }} at the top?\n"
            "   Marts are tables, not views, for dashboard performance."
        )

    def test_fct_orders_naming_prefix(self, fct_orders_path):
        """Fact tables must start with fct_ prefix."""
        assert fct_orders_path.stem.startswith("fct_"), (
            "❌ Fact tables must start with 'fct_' prefix\n"
            "   Expected: fct_orders.sql\n"
            "   Got: " + fct_orders_path.name
        )
