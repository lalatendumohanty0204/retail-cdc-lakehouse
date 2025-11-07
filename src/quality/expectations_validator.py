"""
Great Expectations (programmatic) validations.
We keep this lightweight for the demo, but it's easy to expand into full suites.
"""
from great_expectations.dataset import SparkDFDataset

def run_data_quality_validations(df):
    g = SparkDFDataset(df)

    # Required primary key
    assert g.expect_column_values_to_not_be_null("sale_id").success

    # Sensible ranges prevent garbage data from polluting Silver
    assert g.expect_column_values_to_be_between("quantity", min_value=1, max_value=1000).success
    assert g.expect_column_values_to_be_between("price",    min_value=0.01, max_value=100000.0).success

    # At Silver snapshot level, sale_id should be unique
    assert g.expect_column_values_to_be_unique("sale_id").success
