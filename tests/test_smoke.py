
import pytest

def test_imports():
    import src.etl.bronze_cdc_ingestion as bronze
    import src.etl.silver_sales_refinement as silver
    import src.etl.gold_sales_aggregation as gold
    assert bronze is not None and silver is not None and gold is not None
