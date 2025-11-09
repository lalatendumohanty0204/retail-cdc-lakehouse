# Databricks notebook source
# MAGIC %md
# MAGIC ### Generate Synthetic CDC Events
# MAGIC This step simulates **Debezium-like Change Data Capture (CDC)** events from a retail sales system.
# MAGIC
# MAGIC **Purpose**
# MAGIC - Creates `insert`, `update`, and `delete` operations in JSON format.
# MAGIC - Writes them to the configured **Landing** path (`PATHS["landing"]`).
# MAGIC - Mimics a real source system stream (e.g., Debezium → Kafka → Landing).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

import json, time, uuid, random
from datetime import datetime

class CDCEventSimulator:
    
    def __init__(self, output_path: str, dbutils):
        self.output_path = output_path
        self.dbutils = dbutils
        print(f"Initialized CDCEventSimulator → {output_path}")

    # Generate a synthetic retail transaction record.
    def _build_record(self, sale_id=None):
        return {
            "sale_id": sale_id or str(uuid.uuid4()),
            "customer_id": f"C{random.randint(1, 500):05d}",
            "product_id":  f"P{random.randint(1, 120):05d}",
            "event_ts":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "quantity":    random.choice([1,1,1,2,3,5]),
            "price":       random.choice([99,149,199,499,999,2499])
        }
    
    # Write CDC event samples
    def _write_json(self, filename, record):
        json_str = json.dumps(record)
        file_path = f"{self.output_path}/{filename}"
        self.dbutils.fs.put(file_path, json_str, overwrite=True)

    # Core CDC event logic
    # Build a CDC event of type 'c', 'u', or 'd'.
    def build_cdc_event(self, op):
        if op == "c":
            return {"op":"c","before":None,"after":self._build_record()}
        if op == "u":
            rec = self._build_record()
            rec["quantity"] = max(1, rec["quantity"] + random.choice([-1,1]))
            return {"op":"u","before":None,"after":rec}
        if op == "d":
            return {"op":"d","before":self._build_record(),"after":None}
        raise ValueError(f"Unsupported op: {op}")
    
    # Simulation routines
    # Generate initial 'create' events.
    def generate_initial_load(self, rows=100):
        for _ in range(rows):
            ev = self.build_cdc_event("c")
            self._write_json(f"init_{uuid.uuid4()}.json", ev)
        print(f"Generated {rows} initial CDC records at {self.output_path}")

    # Generate multiple batches of update/delete events.
    def generate_updates(self, num_batches=5, rows_per_batch=20, sleep_seconds=1):
        for i in range(num_batches):
            ops = random.choices(["u","d"], weights=[0.8,0.2], k=rows_per_batch)
            for op in ops:
                ev = self.build_cdc_event(op)
                self._write_json(f"cdc_{uuid.uuid4()}.json", ev)
            print(f"Batch {i+1}/{num_batches} written → {self.output_path}")
            time.sleep(sleep_seconds)



# Initialize the CDC simulator with the landing path
sim = CDCEventSimulator(PATHS["landing"], dbutils)

# Generate a static initial load (historical snapshot)
sim.generate_initial_load(rows=100)

# Generate multiple CDC batches (upserts + deletes)
sim.generate_updates(num_batches=5, rows_per_batch=20, sleep_seconds=0)

# Log output location
print("CDC generated at:", PATHS["landing"])

