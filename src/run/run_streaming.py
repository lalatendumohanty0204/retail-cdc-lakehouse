"""
Convenience runner for Bronze CDC stream ingestion.
Usage:
    python -m src.run.run_streaming
"""
from src.etl.bronze_cdc_ingestion import BronzeCDCIngestionETL

if __name__ == "__main__":
    BronzeCDCIngestionETL().run_cdc_ingestion_stream()
