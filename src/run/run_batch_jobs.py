"""
Convenience runner to rebuild Silver then Gold in one shot.
Usage:
    python -m src.run.run_batch_jobs
"""
from src.etl.silver_sales_refinement import SilverSalesRefinementETL
from src.etl.gold_sales_aggregation import GoldSalesAggregationETL

if __name__ == "__main__":
    SilverSalesRefinementETL().run_sales_refinement()
    GoldSalesAggregationETL().run_sales_aggregation()
