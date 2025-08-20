# consumer_to_pg.py
import json, os, random, psycopg2
from kafka import KafkaConsumer

BOOTSTRAP = "localhost:29092"   # Kafka host listener
DB_DSN = "host=localhost port=5432 dbname=frauddb user=fraud password=fraudpass"

consumer = KafkaConsumer(
    "transactions.raw",
    bootstrap_servers=[BOOTSTRAP],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fraud-loader"
)

conn = psycopg2.connect(DB_DSN)
cur = conn.cursor()

SQL = """
INSERT INTO transactions_raw
(id, ts, customer_id, amount, merchant, city, province, country, mcc, channel, device_id, is_fraud_label)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""

for msg in consumer:
    e = msg.value

    # Generate numeric ID from hash (simplify for demo)
    tx_id = abs(hash(e["tx_id"])) % (10**12)

    # Convert user_id like "Uabcd" → numeric
    cust_id = abs(hash(e["user_id"])) % (10**9)

    # Add some synthetic fields
    merchant = e.get("merchant", random.choice(["Amazon", "Walmart", "Uber", "Starbucks"]))
    city = random.choice(["Toronto", "Vancouver", "Montreal"])
    province = random.choice(["ON", "BC", "QC"])
    country = "CA"
    mcc = random.choice(["5411", "6011", "5812", "4111"])  # grocery, atm, restaurants, transport
    channel = random.choice(["POS", "Online", "Mobile"])
    device_id = f"device-{random.randint(1000,9999)}"

    cur.execute(SQL, (
        tx_id,
        e["ts"],
        cust_id,
        e["amount"],
        merchant,
        city,
        province,
        country,
        mcc,
        channel,
        device_id,
        bool(e["is_fraud"])
    ))
    conn.commit()
    print("inserted tx:", tx_id)
