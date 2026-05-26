# Lab05 - Hệ thống đếm số lượng người sử dụng Kafka và YOLOv8

## 1. Giới thiệu

Bài tập xây dựng hệ thống đếm số lượng người theo thời gian thực sử dụng Kafka và YOLOv8. Hệ thống được triển khai theo kiến trúc phân tán gồm nhiều server độc lập nhằm mô phỏng mô hình xử lý dữ liệu trong môi trường Big Data và Streaming Data Processing.

Mục tiêu của hệ thống:

- Nhận các khung hình từ camera/video
- Truyền dữ liệu frame giữa các server thông qua Kafka
- Thực hiện nhận diện người bằng YOLOv8
- Trả về bounding box của đối tượng được phát hiện
- Lưu trữ kết quả detection phục vụ phân tích dữ liệu

Hệ thống được triển khai theo mô hình pipeline xử lý dữ liệu realtime.

---

# 2. Kiến trúc hệ thống

```text
Camera / Video
        ↓
Camera Server
        ↓
Kafka Topic: camera_frames
        ↓
Processing Server (YOLOv8 Detection)
        ↓
Kafka Topic: detection_results
        ↓
Storage Server
        ↓
JSON + Parquet Output
```

---

# 3. Mô tả luồng hoạt động

## Bước 1 — Camera Server

Camera Server có nhiệm vụ:

- Đọc video đầu vào
- Chia video thành các frame
- Resize frame để giảm tải xử lý
- Encode frame sang định dạng base64
- Gửi dữ liệu frame vào Kafka topic `camera_frames`

Dữ liệu được truyền theo thời gian thực thông qua Kafka.

---

## Bước 2 — Processing Server

Processing Server có nhiệm vụ:

- Nhận frame từ Kafka topic `camera_frames`
- Decode dữ liệu ảnh
- Thực hiện object detection bằng YOLOv8
- Chỉ giữ lại đối tượng thuộc class `person`
- Sinh bounding box cho từng người được phát hiện
- Tính số lượng người trong frame
- Gửi kết quả detection vào Kafka topic `detection_results`

Kết quả detection bao gồm:

- frame_id
- timestamp
- person_count
- bounding boxes
- confidence score

---

## Bước 3 — Storage Server

Storage Server có nhiệm vụ:

- Nhận kết quả detection từ Kafka
- Lưu dữ liệu dưới dạng JSON
- Lưu dữ liệu dưới dạng Apache Parquet

Dữ liệu được lưu phục vụ:

- phân tích dữ liệu
- thống kê
- visualization
- big data analytics

---

# 4. Công nghệ sử dụng

| Công nghệ | Vai trò |
|---|---|
| Python | Ngôn ngữ lập trình chính |
| OpenCV | Xử lý video và frame |
| Apache Kafka | Streaming dữ liệu realtime |
| YOLOv8 | Nhận diện đối tượng |
| Pandas | Xử lý dữ liệu |
| Apache Parquet | Lưu trữ dữ liệu lớn |
| Docker | Triển khai Kafka bằng container |

---

# 5. Cấu trúc thư mục

```text
Lab5/
│
├── data/
│   └── input.mp4
│
├── output/
│   ├── people_count_result.json
│   └── people_count_result.parquet
│
├── server_camera/
│   └── camera_server.py
│
├── server_processing/
│   └── processing_server.py
│
├── server_storage/
│   └── storage_server.py
│
├── utils/
│   ├── config.py
│   └── read_result.py
│
├── requirements.txt
├── docker-compose.yml
├── README.md
└── .gitignore
```

---

# 6. Mô tả các thành phần

## 6.1 Camera Server

File:

```text
server_camera/camera_server.py
```

Chức năng:

- Đọc video từ thư mục `data`
- Resize frame
- Encode dữ liệu ảnh
- Gửi dữ liệu sang Kafka

Kafka topic sử dụng:

```text
camera_frames
```

---

## 6.2 Processing Server

File:

```text
server_processing/processing_server.py
```

Chức năng:

- Nhận frame từ Kafka
- Decode frame
- Thực hiện detection bằng YOLOv8
- Sinh bounding box
- Đếm số lượng người
- Gửi kết quả detection sang Kafka

Kafka topic sử dụng:

```text
detection_results
```

---

## 6.3 Storage Server

File:

```text
server_storage/storage_server.py
```

Chức năng:

- Nhận dữ liệu detection
- Lưu JSON
- Lưu Parquet

Output:

```text
output/people_count_result.json
output/people_count_result.parquet
```

---

# 7. Ngữ cảnh Big Data

Dự án được triển khai trong ngữ cảnh hệ thống dữ liệu lớn.

## 7.1 Apache Kafka

Kafka được sử dụng như một distributed streaming platform nhằm truyền dữ liệu realtime giữa các server.

Ưu điểm:

- Hỗ trợ xử lý dữ liệu realtime
- Kiến trúc phân tán
- Throughput cao
- Dễ mở rộng hệ thống
- Tách biệt các server xử lý

---

## 7.2 Apache Parquet

Kết quả detection được lưu dưới định dạng Apache Parquet.

Ưu điểm:

- Columnar storage
- Tối ưu cho Big Data Analytics
- Tốc độ đọc/ghi nhanh
- Tiết kiệm dung lượng lưu trữ
- Phù hợp cho Spark/Pandas/Data Warehouse

---

# 8. Cài đặt hệ thống

## 8.1 Tạo môi trường ảo

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

## 8.2 Cài đặt thư viện

```bash
python -m pip install -r requirements.txt
```

---

## 8.3 Khởi động Kafka

```bash
docker-compose up -d
```

Kiểm tra container:

```bash
docker ps
```

---

# 9. Chạy hệ thống

## Bước 1 — Chạy Storage Server

```bash
python server_storage/storage_server.py
```

---

## Bước 2 — Chạy Processing Server

```bash
python server_processing/processing_server.py
```

---

## Bước 3 — Chạy Camera Server

```bash
python server_camera/camera_server.py
```

---

# 10. Đọc kết quả

Chạy:

```bash
python utils/read_result.py
```

Ví dụ kết quả:

```text
========== STATISTICS ==========

Total frames: 175
Average people: 3.0
Max people: 4
```

---

# 11. File kết quả

| File | Mô tả |
|---|---|
| people_count_result.json | Kết quả detection dạng JSON |
| people_count_result.parquet | Kết quả detection dạng Parquet |

---

# 12. Ví dụ dữ liệu detection

```json
{
    "frame_id": 0,
    "timestamp": 1716800000,
    "person_count": 3,
    "bboxes": [
        {
            "x1": 84,
            "y1": 56,
            "x2": 189,
            "y2": 330,
            "confidence": 0.92
        }
    ]
}
```

---

# 13. Kết quả đạt được

Hệ thống đã:

- Xây dựng thành công pipeline xử lý phân tán
- Truyền dữ liệu realtime bằng Kafka
- Nhận diện người bằng YOLOv8
- Sinh bounding box chính xác
- Đếm số lượng người trong từng frame
- Lưu dữ liệu dưới định dạng Big Data

---

# 14. Hướng phát triển

Hệ thống có thể mở rộng cho:

- Multi-camera systems
- Smart Surveillance
- Crowd Monitoring
- Smart City Analytics
- Traffic Monitoring
- Realtime Analytics Systems

---

# 15. Kết luận

Bài tập đã triển khai thành công hệ thống đếm số lượng người theo kiến trúc phân tán sử dụng Kafka và YOLOv8.

Hệ thống hỗ trợ:

- Streaming realtime
- Xử lý phân tán
- Object detection
- Big Data storage
- Realtime analytics pipeline

Kiến trúc của hệ thống phù hợp cho các bài toán xử lý dữ liệu lớn và hệ thống giám sát thông minh trong thực tế.