# kaggle_streamer.py
import csv, json, time, uuid, datetime, os, hashlib
from kafka import KafkaProducer

CSV_PATH = r"D:\Project\real-time-fraud-analytics\data\kaggle\creditcard.csv"
TOPIC = "transactions.raw"

# If running on Windows host: use localhost:9092
# If running from a container on the docker network: use kafka:9092
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:29092")
SPEED = float(os.getenv("SPEED", "100"))  # rows/sec

def to_event(row):
    seed = row["Time"] or str(uuid.uuid4())
    user_hash = hashlib.md5(seed.encode()).hexdigest()[:8]
    base = datetime.datetime.utcnow()
    try:
        ts = base + datetime.timedelta(seconds=int(float(row["Time"])))
    except:
        ts = base
    return {
        "tx_id": str(uuid.uuid4()),
        "user_id": f"U{user_hash}",
        "amount": float(row["Amount"]),
        "currency": "EUR",
        "ts": ts.isoformat() + "Z",
        "features": {k: float(row[k]) for k in row if k.startswith("V")},
        "is_fraud": int(row["Class"])
    }

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(CSV_PATH, newline="") as f:
    reader = csv.DictReader(f)
    interval = 1.0 / SPEED if SPEED > 0 else 0
    for row in reader:
        evt = to_event(row)
        producer.send(TOPIC, evt)
        print("sent", evt["tx_id"], evt["amount"], evt["is_fraud"])
        if interval > 0:
            time.sleep(interval)
