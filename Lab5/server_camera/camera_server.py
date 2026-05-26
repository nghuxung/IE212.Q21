import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import cv2
import base64
import json
import time
from kafka import KafkaProducer
from utils.config import KAFKA_SERVER, CAMERA_TOPIC, VIDEO_PATH, FRAME_WIDTH, FRAME_HEIGHT


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

cap = cv2.VideoCapture(VIDEO_PATH)

if not cap.isOpened():
    raise Exception(f"Không mở được video: {VIDEO_PATH}")

frame_id = 0

print("Camera server started...")

while True:
    ret, frame = cap.read()

    if not ret:
        print("Đã đọc hết video.")
        break

    frame = cv2.resize(frame, (FRAME_WIDTH, FRAME_HEIGHT))

    _, buffer = cv2.imencode(".jpg", frame)
    frame_base64 = base64.b64encode(buffer).decode("utf-8")

    message = {
        "frame_id": frame_id,
        "timestamp": time.time(),
        "frame": frame_base64
    }

    producer.send(CAMERA_TOPIC, message)

    print(f"Sent frame {frame_id}")

    frame_id += 1
    time.sleep(0.05)

cap.release()
producer.flush()
producer.close()

print("Camera server stopped.")