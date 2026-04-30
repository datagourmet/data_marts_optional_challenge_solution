"""
Test Challenge 06 — Checkpoint 3: Full Mart Layer

Validates the complete mart layer:
- Both dim_customers.sql and fct_orders.sql exist
- Naming conventions (fct_/dim_ prefixes)
- models/marts/schema.yml documents both models
- dbt_project.yml has a 'marts:' config section

Run: pytest tests/test_all_marts.py -v
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


class TestAllMarts:
    """Checkpoint 3: Validate documentation and full mart layer runs correctly."""

    @pytest.fixture
    def mart_sql_files(self, marts_dir):
        """Get all SQL model files in the marts directory."""
        sql_files = list(marts_dir.rglob("*.sql"))
        return [f for f in sql_files if f.stem not in ['schema', 'sources']]

    @pytest.fixture
    def schema_yml_path(self, marts_dir):
        """Path to models/marts/schema.yml."""
        return marts_dir / "schema.yml"

    @pytest.fixture
    def schema_content(self, schema_yml_path):
        """Content of schema.yml if it exists."""
        if not schema_yml_path.exists():
            return ""
        with open(schema_yml_path, 'r') as f:
            return f.read()

    def test_both_mart_models_exist(self, marts_dir):
        """Both dim_customers.sql and fct_orders.sql must exist."""
        dim = (marts_dir / "dim_customers.sql").exists()
        fct = (marts_dir / "fct_orders.sql").exists()
        assert dim and fct, (
            "❌ Both mart models must exist\n"
            "   Missing:\n" +
            ("   - models/marts/dim_customers.sql\n" if not dim else "") +
            ("   - models/marts/fct_orders.sql\n" if not fct else "") +
            "   Did you complete both Sections 2 and 3?"
        )

    def test_mart_models_naming_convention(self, mart_sql_files):
        """All mart models must use fct_ or dim_ prefix."""
        wrong = [f.stem for f in mart_sql_files
                 if not f.stem.startswith(('fct_', 'dim_'))]
        assert len(wrong) == 0, (
            "❌ All mart models must start with 'fct_' or 'dim_' prefix\n"
            "   Models without correct prefix:\n" +
            "\n".join([f"   - {name}" for name in wrong]) +
            "\n   Use 'fct_' for fact tables and 'dim_' for dimension tables."
        )

    def test_schema_yml_exists(self, schema_yml_path):
        """Must create models/marts/schema.yml to document mart models."""
        assert schema_yml_path.exists(), (
            "❌ models/marts/schema.yml not found\n"
            "   Did you create the documentation file? (Section 7)\n"
            "   Run: code models/marts/schema.yml"
        )

    def test_schema_yml_documents_dim_customers(self, schema_content):
        """schema.yml must document dim_customers."""
        assert "dim_customers" in schema_content, (
            "❌ schema.yml must include documentation for dim_customers\n"
            "   Did you add a dim_customers entry under 'models:'?"
        )

    def test_schema_yml_documents_fct_orders(self, schema_content):
        """schema.yml must document fct_orders."""
        assert "fct_orders" in schema_content, (
            "❌ schema.yml must include documentation for fct_orders\n"
            "   Did you add a fct_orders entry under 'models:'?"
        )

    def test_dbt_project_yml_has_marts_config(self, project_dir):
        """dbt_project.yml must have a marts materialization config."""
        import yaml
        dbt_project = project_dir / "dbt_project.yml"
        assert dbt_project.exists(), (
            "❌ dbt_project.yml not found in jaffle_shop_dbt/\n"
            "   Is this the correct project directory?"
        )
        with open(dbt_project, 'r') as f:
            cfg = yaml.safe_load(f)
        project_name = cfg.get('name', 'jaffle_shop_dbt')
        models_cfg = cfg.get('models', {}).get(project_name, {})
        assert 'marts' in models_cfg, (
            "❌ dbt_project.yml must have a 'marts:' section under models:\n"
            "   Did you add the marts configuration? (Section 6.1)\n"
            "   Marts need materialized: table and schema: marts"
        )
