# Clean Address Data Pipeline

## Giới thiệu
Dự án này thực hiện tiền xử lý dữ liệu địa chỉ từ các nguồn khác nhau, làm sạch và chuẩn hóa dữ liệu để sử dụng cho các mô hình phân tích hoặc machine learning.

## Cách sử dụng
### 1. Cài đặt yêu cầu
Trước khi chạy pipeline, bạn cần cài đặt các thư viện cần thiết bằng cách chạy lệnh:

pip install -r requirements.txt

### 2. Chạy pipeline
Chạy lệnh sau trong terminal để thực hiện pipeline:
python script.py
Sau khi chạy thành công, dữ liệu đã qua xử lý sẽ được lưu vào file processed_data.csv.
## Luồng xử lý dữ liệu
Pipeline bao gồm các bước sau:
  1. Load dữ liệu từ các file Excel:

    Khach_hang_Doi_tac.xlsx
    LocationId.xlsx
    Nha_cung_cap.xlsx  
2. Lọc các cột cần thiết:
   
    CUST_CODE, CUST_ADDR từ Khach_hang_Doi_tac.xlsx
    LS_ACC_FLEX_01, LS_ACC_FLEX_01_DESC từ LocationId.xlsx
    ADDR_CODE, ADDR_LINE_1 từ Nha_cung_cap.xlsx
3. Gộp dữ liệu từ các nguồn
  Dữ liệu từ ba file được hợp nhất thành một DataFrame chứa các cột:

    address_id
   
    raw_address
4. Làm sạch dữ liệu địa chỉ (raw_address)
  - Xóa giá trị NULL
  - Loại bỏ giá trị trùng lặp
  - Lọc địa chỉ không hợp lệ:
  - Địa chỉ quá ngắn (dưới 12 ký tự)
  - Địa chỉ chứa ký tự ?
  - Địa chỉ không phải tiếng Việt (dùng thư viện langdetect)
5. Kiểm tra và sửa address_id
  - Xác định ID hợp lệ (chỉ số nguyên hoặc chuỗi số)
  - Tạo ID mới nếu cần thiết (nếu thiếu hoặc chứa ký tự không hợp lệ)
  - Chuẩn hóa ID (xóa ký tự không hợp lệ, chuyển đổi sang số nếu cần)
  6. Lưu kết quả
  Dữ liệu sau khi xử lý được lưu vào processed_data.csv.



