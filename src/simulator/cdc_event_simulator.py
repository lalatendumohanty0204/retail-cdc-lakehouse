"""
CDCEventSimulator
----------------------------------------
Simulates Debezium-style Change Data Capture (CDC) events and writes them as JSON files into a DBFS landing path. 

This mimics a real Debezium → Kafka → Landing workflow, without any external infrastructure.
"""

import json, time, uuid, random
from datetime import datetime
from src.utils.logging_utils import create_pipeline_logger

class CDCEventSimulator:

    def __init__(self, output_path: str, dbutils=None):
        if not output_path.startswith("dbfs:/"):
            raise ValueError(
                f"Invalid output path: {output_path}. "
            )

        self.output_path = output_path.rstrip("/") 
        self.dbutils = dbutils
        self.log = create_pipeline_logger("CDCEventSimulator")

        # Ensure DBFS directory exists        
        try:
            self.dbutils.fs.mkdirs(self.output_path)
            self.log.info(f"DBFS path verified: {self.output_path}")
        except Exception as e:
            self.log.error(f"Failed to create DBFS directory {self.output_path}: {e}")
            raise

    def _build_record(self, sale_id=None):
        """Generate a synthetic retail transaction record."""
        return {
            "sale_id": sale_id or str(uuid.uuid4()),
            "customer_id": f"C{random.randint(1, 500):05d}",
            "product_id": f"P{random.randint(1, 120):05d}",
            "event_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": random.choice([1, 1, 1, 2, 3, 5]),
            "price": random.choice([99, 149, 199, 499, 999, 2499])
        }

    def _write_json(self, filename: str, record: dict):
        """Write a JSON file to DBFS."""
        json_str = json.dumps(record)
        self.dbutils.fs.put(f"{self.output_path}/{filename}", json_str, overwrite=True)

    # Core CDC event logic
    # Build a CDC event of type 'c', 'u', or 'd'.
    def build_cdc_event(self, op: str):
        if op == "c":
            return {"op": "c", "before": None, "after": self._build_record()}
        if op == "u":
            rec = self._build_record()
            rec["quantity"] = max(1, rec["quantity"] + random.choice([-1, 1]))
            return {"op": "u", "before": None, "after": rec}
        if op == "d":
            return {"op": "d", "before": self._build_record(), "after": None}
        raise ValueError(f"Unsupported operation: {op}")

    # Simulation routines
    # Generate initial 'create' events.
    def generate_initial_load(self, rows=100):
        for _ in range(rows):
            ev = self.build_cdc_event("c")
            self._write_json(f"init_{uuid.uuid4()}.json", ev)
        self.log.info(f"Generated {rows} initial CDC records at {self.output_path}")

    # Generate multiple batches of update/delete events.
    def generate_updates(self, num_batches=5, rows_per_batch=20, sleep_seconds=1):
        for i in range(num_batches):
            ops = random.choices(["u", "d"], weights=[0.8, 0.2], k=rows_per_batch)
            for op in ops:
                ev = self.build_cdc_event(op)
                self._write_json(f"cdc_{uuid.uuid4()}.json", ev)
            self.log.info(f"Batch {i+1}/{num_batches} written → {self.output_path}")
            time.sleep(sleep_seconds)
