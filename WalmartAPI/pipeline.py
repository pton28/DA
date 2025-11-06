import subprocess
import os
from dotenv import load_dotenv

# === Load biến môi trường từ file .env ===
load_dotenv()
api_key = os.getenv("API_KEY")

if not api_key:
    raise ValueError("⚠️ Không tìm thấy API_KEY trong file .env! Hãy tạo file .env và thêm API_KEY=...")

# === Danh sách các bước cần chạy tuần tự ===
steps = [
    "src/call_API.py",
    "src/save_data.py",
    "src/analyze_data.py",   
    "src/clean_data.py",
    "src/eda_api.py"
]

print("Bắt đầu chạy toàn bộ pipeline...\n")

for step in steps:
    print(f"Đang chạy: {step}")
    result = subprocess.run(["python", step], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"Lỗi ở bước: {step}")
        print(result.stderr)
        break

print("\nPipeline hoàn tất! Kiểm tra thư mục 'data/' để xem kết quả.")
