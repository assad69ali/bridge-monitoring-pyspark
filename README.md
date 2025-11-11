🏗️ Bridge Monitoring PySpark Pipeline

A PySpark structured streaming data pipeline simulating IoT sensor monitoring for bridges.
It processes real-time temperature, vibration, and tilt readings through Bronze → Silver → Gold layers, applying data validation and aggregation to produce final metrics.
bridge-monitoring-pyspark/
│
├── data_generator/              # synthetic data generator for bridge sensors
├── metadata/
│   └── bridge_metadata.csv      # static bridge info (bridge_id, name, location, etc.)
├── pipelines/
│   ├── bronze_ingest.py         # raw JSON ingestion and basic validation
│   ├── silver_enrichment.py     # enrichment with metadata + range checks
│   └── gold_aggregation.py      # windowed aggregations and joins
│
├── bridge_pipeline_notebook.ipynb  # demo notebook (Colab)
├── README.md
└── requirements.txt  (optional)
⚙️ Overview of Pipeline Layers
🥉 Bronze Layer — Raw Ingestion

Reads streaming JSON data from /streams/bridge_*

Validates non-null event_time and value

Writes valid data to bronze/bridge_*

Invalid (nulls, malformed) → bronze/rejected

bronze/
 ├── bridge_temperature/
 ├── bridge_vibration/
 ├── bridge_tilt/
 └── rejected/
🥇 Gold Layer — Aggregations

Reads 3 Silver streams.

Applies withWatermark("event_time_ts", "2 minutes")

Computes 1-minute tumbling window aggregates:

avg_temperature

max_vibration

max_tilt_angle

Joins on (bridge_id, window_start, window_end) to produce bridge_metrics

Writes output to gold/bridge_metrics/

A validation notebook cell calculates join success:
from pyspark.sql.functions import col, when, sum as spark_sum
silver_df = spark.read.parquet("silver/bridge_temperature", "silver/bridge_vibration", "silver/bridge_tilt")
bridges_df = spark.read.csv("metadata/bridge_metadata.csv", header=True)
joined = silver_df.join(bridges_df, "bridge_id", "left")
matched = joined.filter(col("name").isNotNull()).count()
total = joined.count()
print("Join success rate:", round(100 * matched / total, 2), "%")

🧪 Testing & Evaluation

Deterministic test mode in data_generator (use test_seed).

Watermark experiments:

Too small watermark → late events dropped.

Too large → latency increases but completeness improves.

Unit checks (e.g., foreachBatch validation, late event handling).

🚀 Running the Pipeline
from data_generator.data_generator import start_bridge_generator
from pipelines.bronze_ingest import main as bronze_main
from pipelines.silver_enrichment import main as silver_main
from pipelines.gold_aggregation import main as gold_main

# 1. Generate mock stream data
start_bridge_generator(stream_dir="/content/streams", duration_seconds=30, rate_per_sec=10)

# 2. Run bronze layer
bronze_main(30)

# 3. Run silver layer
silver_main(30)

# 4. Run gold layer
gold_main()

