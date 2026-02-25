# Idempotency in Data Pipelines

Code examples for the Medium article: **[Idempotency in Data Pipelines](#)**

This repo demonstrates **why idempotency matters** in data pipelines with three PySpark examples:

| # | Script | Pattern | Idempotent? |
|---|--------|---------|:-----------:|
| 1 | `01_bad_append.py` | Blind append | No |
| 2 | `02_good_partition_overwrite.py` | Dynamic partition overwrite | Yes |
| 3 | `03_good_delta_merge.py` | Delta Lake MERGE (upsert) | Yes |

---

## Prerequisites

- **Python 3.9+**
- **Java 8 or 11** (required by Spark)

## Getting Started

```bash
git clone https://github.com/<your-username>/data-pipeline-idempotency.git
cd data-pipeline-idempotency

python -m venv venv
source venv/bin/activate   # Or if you are using Windows: venv\Scripts\activate

pip install -r requirements.txt

# Generate sample data
python data/generate_orders.py
```

This creates two CSV files under `data/input/`:
- `orders.csv` — 20 order records across two dates
- `orders_redelivery.csv` — same batch with updated quantities (simulates corrections)

---

## Example 1: The Bad Way (Blind Append)

```bash
python examples/01_bad_append.py          # first run:  10 rows
python examples/01_bad_append.py          # second run: 20 rows — duplicates!
```

Each run appends the same rows again. If your scheduler retries or you re-run the pipeline, you get duplicate data.

```bash
rm -rf output/bad_append    # clean up
```

---

## Example 2: Partition Overwrite

```bash
python examples/02_good_partition_overwrite.py    # first run:  10 rows
python examples/02_good_partition_overwrite.py    # second run: 10 rows — same!
```

Spark overwrites only the partition(s) present in the incoming data, leaving other partitions untouched. The key config:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

```bash
rm -rf output/partition_overwrite    # clean up
```

---

## Example 3: Delta Lake MERGE (Upsert)

```bash
python examples/03_good_delta_merge.py                # initial load: 20 rows
python examples/03_good_delta_merge.py --redelivery   # re-delivery: still 20 rows, but quantities updated
```

The MERGE matches on `order_id` — existing rows get updated, new rows get inserted. The row count stays the same, but data reflects the latest corrections.

```bash
rm -rf output/delta_orders    # clean up
```

---

## Project Structure

```
data-pipeline-idempotency/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   ├── generate_orders.py          # sample data generator
│   └── input/                      # generated at runtime (.gitignored)
│       ├── orders.csv
│       └── orders_redelivery.csv
├── examples/
│   ├── 01_bad_append.py            # non-idempotent blind append
│   ├── 02_good_partition_overwrite.py
│   └── 03_good_delta_merge.py
└── output/                         # generated at runtime (.gitignored)
```

---

## License

MIT