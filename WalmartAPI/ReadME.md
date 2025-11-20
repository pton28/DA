# 🛒 Walmart Data Pipeline

Dự án Python giúp tự động thu thập, lưu trữ, phân tích và trực quan hóa dữ liệu sản phẩm từ **Walmart** thông qua **SerpAPI**.  
Mọi người có thể chạy toàn bộ pipeline chỉ với **1 lệnh duy nhất**, hoặc mở trực tiếp bằng **VS Code Dev Container**.

---

## 📂 Cấu trúc thư mục

WalmartAPI/
│
├── src/
│ ├── call_API.py # Bước 1 - Gọi API lấy dữ liệu thô
│ ├── save_data.py # Bước 2 - Lưu dữ liệu ra CSV, JSON, Excel
│ ├── analyze_data.py # Bước 3 - Phân tích thống kê cơ bản
│ ├── clean_data.py # Bước 4 - Làm sạch dữ liệu
│ ├── eda_api.py # Bước 5 - Phân tích EDA và vẽ biểu đồ
│
├── data/
│ ├── raw_data/ # Dữ liệu gốc từ API
│ ├── save_data/ # Dữ liệu lưu lần đầu
│ ├── clean_data/ # Dữ liệu đã làm sạch
│ └── eda_pic/ # Hình ảnh và bảng phân tích EDA
│
├── pipeline.py # Chạy toàn bộ 5 bước pipeline
├── requirements.txt # Danh sách thư viện cần cài
├── Dockerfile # Định nghĩa image Docker
├── .dockerignore # Bỏ qua file/thư mục không cần khi build image
├── .env # File chứa API_KEY (bảo mật, không push)
├── .env.example # Mẫu file .env cho người khác sử dụng
└── .devcontainer/
└── devcontainer.json # Cấu hình cho VS Code Dev Container

yaml
Sao chép mã

---

## ⚙️ Cài đặt môi trường

### 1️⃣ Tạo file `.env`

Tạo file `.env` trong thư mục gốc (hoặc copy từ mẫu):
```bash
cp .env.example .env
Rồi thêm API key của bạn:

API_KEY=your_serpapi_key_here

⚠️ File .env không được commit để bảo mật.

2️⃣ Cài thư viện (nếu chạy local, không dùng Docker)

pip install -r requirements.txt

🚀 Chạy pipeline (local)

Chỉ cần một lệnh duy nhất:

python pipeline.py

Pipeline sẽ tự động chạy lần lượt:

call_API.py → Gọi API và lưu raw data

save_data.py → Lưu dữ liệu thô ra file

analyze_data.py → Phân tích thống kê cơ bản

clean_data.py → Làm sạch dữ liệu

eda_api.py → Vẽ biểu đồ và lưu kết quả

Toàn bộ kết quả sẽ nằm trong thư mục data/.

🐳 Chạy bằng Docker (cách 1 — thủ công)
1️⃣ Build image
docker build -t walmart-pipeline .

2️⃣ Chạy container
Trên Linux/macOS:
docker run --env-file .env -v ${PWD}/data:/app/data walmart-pipeline


Trên Windows PowerShell:
docker run --env-file .env -v "%cd%/data:/app/data" walmart-pipeline


Docker sẽ:

Đọc .env để lấy API_KEY

Mount thư mục data để lưu kết quả

Chạy toàn bộ pipeline tự động

💻 Chạy bằng VS Code Dev Container (cách 2 — khuyến nghị)
Cách này đơn giản nhất cho nhóm học tập hoặc teamwork.

Chuẩn bị:
Cài extension:

🐳 Docker

🧱 Dev Containers

Clone project:
git clone https://github.com/<your-username>/WalmartAPI.git
cd WalmartAPI
cp .env.example .env

Mở project bằng VS Code

Thực hiện:
Khi được hỏi → chọn “Reopen in Container”

VS Code sẽ tự:

Build Docker image từ Dockerfile

Cài tất cả thư viện

Mount thư mục data

Load .env (để có API_KEY)

Sau khi container mở xong, vào Terminal trong VS Code và chạy:

python pipeline.py

➡️ Tất cả 5 bước pipeline sẽ chạy tự động trong môi trường container.

📊 Kết quả đầu ra
Thư mục	Nội dung
data/raw_data	Dữ liệu gốc (raw JSON) từ API
data/save_data	Dữ liệu lưu định dạng CSV, JSON, Excel
data/clean_data	Dữ liệu đã làm sạch
data/eda_pic	Hình ảnh và bảng phân tích EDA



import duckdb
import pandas as pd

db_path = r"C:\Users\LENOVO\Downloads\DA_pipeline\DA\database\walmart_analytics.db"
conn = duckdb.connect(db_path, read_only=True)

# Load all dimensions
DIM_CATEGORY = conn.execute("SELECT * FROM DIM_CATEGORY").df()
DIM_CUSTOMER = conn.execute("SELECT * FROM DIM_CUSTOMER").df()
DIM_DATE = conn.execute("SELECT * FROM DIM_DATE").df()
DIM_PAYMENT = conn.execute("SELECT * FROM DIM_PAYMENT").df()
DIM_PRODUCT = conn.execute("SELECT * FROM DIM_PRODUCT").df()
DIM_SELLER = conn.execute("SELECT * FROM DIM_SELLER").df()

# Load fact table (ONLY FACT_SALES - the real transaction data)
FACT_SALES = conn.execute("SELECT * FROM FACT_SALES").df()

conn.close(). Nghiên cứu viết lại lệnh này xem nhé. Chứ lệnh bạn gửi khi chạy load dữ