"""
PipelineETLBase: a lightweight base for ETL stages (no ABC to keep it simple).

Why no abstract base class here?
- Simpler for a small project; we still raise NotImplementedError to enforce overrides.
- Easier to read in interviews and friendly to quick extensions.

Each concrete ETL can implement: extract() -> transform() -> load()
Streaming ETLs may expose a dedicated start()/await() instead of run().
"""
from typing import Any
from src.utils.logging_utils import create_pipeline_logger

class PipelineETLBase:
    def __init__(self, etl_name: str):
        self.etl_name = etl_name
        self.log = create_pipeline_logger(etl_name)

    def extract(self) -> Any:
        raise NotImplementedError("extract() must be implemented by the ETL stage.")

    def transform(self, df: Any) -> Any:
        raise NotImplementedError("transform() must be implemented by the ETL stage.")

    def load(self, df: Any) -> None:
        raise NotImplementedError("load() must be implemented by the ETL stage.")

    def run(self) -> None:
        """Standard batch lifecycle; streaming stages should provide start()/awaitTermination()."""
        self.log.info("Starting ETL stage run()")
        data = self.extract()
        data = self.transform(data)
        self.load(data)
        self.log.info("ETL stage completed successfully")
