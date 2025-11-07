"""
CDCEventSimulator
-----------------
Generates Debezium-like CDC change events as newline-delimited JSON files
in the landing folder. This mimics Oracle→Debezium→Kafka without infrastructure.

Event shape:
{
  "op": "c" | "u" | "d",
  "before": {...} or null,
  "after":  {...} or null
}
"""
import json, time, uuid, random
from datetime import datetime
from pathlib import Path
from src.utils.logging_utils import create_pipeline_logger

log = create_pipeline_logger("CDCEventSimulator")
LANDING = Path("data/landing"); LANDING.mkdir(parents=True, exist_ok=True)

def _build_record(sale_id=None):
    return {
        "sale_id": sale_id or str(uuid.uuid4()),
        "customer_id": f"C{random.randint(1, 500):05d}",
        "product_id":  f"P{random.randint(1, 120):05d}",
        "event_ts":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "quantity":    random.choice([1,1,1,2,3,5]),
        "price":       random.choice([99,149,199,499,999,2499])
    }

def build_cdc_event(op: str):
    if op == "c":
        return {"op":"c","before":None,"after":_build_record()}
    if op == "u":
        rec = _build_record()
        rec["quantity"] = max(1, rec["quantity"] + random.choice([-1,1]))
        return {"op":"u","before":None,"after":rec}
    if op == "d":
        return {"op":"d","before":_build_record(),"after":None}
    raise ValueError("Unsupported op")

def main(rate_per_sec: float = 1.0):
    interval = 1.0 / max(rate_per_sec, 0.1)
    log.info(f"Starting CDC simulator at ~{rate_per_sec:.2f} ev/s")
    while True:
        op = random.choices(["c","u","d"], weights=[0.65,0.25,0.10])[0]
        ev = build_cdc_event(op)
        out = LANDING / f"cdc_{uuid.uuid4()}.json"
        out.write_text(json.dumps(ev))
        log.info(f"Wrote {op} → {out.name}")
        time.sleep(interval)

if __name__ == "__main__":
    main(1.0)
