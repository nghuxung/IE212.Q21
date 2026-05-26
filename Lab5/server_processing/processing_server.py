import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cv2
import json
import base64
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from ultralytics import YOLO
from utils.config import KAFKA_SERVER, CAMERA_TOPIC, RESULT_TOPIC


model = YOLO("yolov8n.pt")

consumer = KafkaConsumer(
    CAMERA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="processing-server"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Processing server started...")

for msg in consumer:
    data = msg.value

    frame_bytes = base64.b64decode(data["frame"])
    np_arr = np.frombuffer(frame_bytes, np.uint8)
    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    results = model(frame, verbose=False)[0]

    bboxes = []

    for box in results.boxes:
        class_id = int(box.cls[0])

        if class_id == 0:
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            confidence = float(box.conf[0])

            bboxes.append({
                "x1": int(x1),
                "y1": int(y1),
                "x2": int(x2),
                "y2": int(y2),
                "confidence": round(confidence, 4)
            })

    result_message = {
        "frame_id": data["frame_id"],
        "timestamp": data["timestamp"],
        "person_count": len(bboxes),
        "bboxes": bboxes
    }

    producer.send(RESULT_TOPIC, result_message)

    print(
        f"Frame {data['frame_id']} | "
        f"People: {len(bboxes)} | "
        f"BBoxes: {bboxes}"
    )