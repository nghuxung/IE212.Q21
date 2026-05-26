import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pandas as pd
from kafka import KafkaConsumer
from utils.config import KAFKA_SERVER, RESULT_TOPIC, JSON_OUTPUT, PARQUET_OUTPUT


os.makedirs("output", exist_ok=True)

consumer = KafkaConsumer(
    RESULT_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="storage-server"
)

results = []

print("Storage server started...")

for msg in consumer:
    data = msg.value
    results.append(data)

    with open(JSON_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    df = pd.DataFrame(results)
    df.to_parquet(PARQUET_OUTPUT, index=False)

    print(
        f"Saved frame {data['frame_id']} | "
        f"people_count = {data['person_count']}"
    )