# ==== Base Image ====
FROM python:3.10-slim

# ==== Set working directory ====
WORKDIR /app

# ==== Copy toàn bộ project ====
COPY . /app

# ==== Cài đặt dependencies ====
RUN pip install --no-cache-dir -r requirements.txt

# ==== Tạo thư mục data ====
RUN mkdir -p /app/data/raw_data /app/data/save_data /app/data/clean_data /app/data/eda_pic

# ==== Cho phép đọc file .env nếu có ====
COPY .env .env

# ==== Chạy toàn bộ pipeline (đủ 5 bước) ====
CMD ["sh", "-c", "python src/call_API.py && python src/save_data.py && python src/analyze_data.py && python src/clean_data.py && python src/eda_api.py"]
