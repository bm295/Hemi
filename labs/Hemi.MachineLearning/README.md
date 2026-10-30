# Hemi Machine Learning Lab

Bài thực hành supervised learning: dự đoán số phần bán của mỗi món cho ngày
kế tiếp. Đây là project Python console độc lập trong repo Hemi, dùng
scikit-learn để dễ so sánh mô hình; không thay đổi API hay workflow .NET.

## Chạy từ thư mục gốc repo

Yêu cầu Python 3.11 trở lên. Nên cài dependencies trong virtual environment.

```bash
python -m pip install -r labs/Hemi.MachineLearning/requirements.txt
python labs/Hemi.MachineLearning/lab.py
python -m unittest discover -s labs/Hemi.MachineLearning -v
```

Lệnh mặc định tự sinh 365 ngày × 3 món với seed 42. Dữ liệu có quy luật
cuối tuần và nhiễu ngẫu nhiên; hoàn toàn mô phỏng, không phải doanh số Hemi.
Kết quả được in ra console và lưu ở `output/metrics.json`; dữ liệu đầu vào
được lưu ở `output/daily-sales.csv`. Không cần SQL Server hoặc chạy ứng dụng.

## Ba khái niệm cần hiểu

1. **Supervised learning:** X gồm mã món, thứ, lượng bán hôm trước,
   trung bình 7 ngày trước và trung bình 4 ngày cùng thứ trước đó.
   y là lượng bán thực tế của ngày cần dự đoán. Mô hình học quan hệ X → y.
2. **Train/validation/test:** bỏ 28 ngày đầu để tạo lịch sử, rồi chia theo
   ngày theo tỷ lệ xấp xỉ 67%/16%/17%. Tất cả món cùng ngày thuộc cùng tập.
   Train học tham số và bộ mã hóa; validation chọn mô hình/độ sâu;
   chỉ mô hình đã chọn được đánh giá trên test. Mô hình giữ nguyên sau train.
3. **Overfitting:** so sánh MAE train và validation khi tăng độ sâu cây
   từ 2, 4, 8 đến không giới hạn. Train tốt hơn nhưng validation xấu hơn
   cho thấy mô hình đang học cả nhiễu. MAE có đơn vị phần món, càng thấp càng tốt.

Baseline dự đoán bằng trung bình 4 ngày cùng thứ trước đó. Các ứng viên
gồm baseline, hồi quy tuyến tính và các cây; tất cả được chọn bằng validation.
Không bảo đảm ML sẽ thắng baseline trên dữ liệu khác.

## Đọc kết quả mẫu (seed 42)

| Mô hình | MAE train | MAE validation |
|---|---:|---:|
| Baseline | 4.512 | 4.454 |
| Linear | 3.992 | 3.944 |
| Cây sâu 4 | 4.048 | 4.366 |
| Cây sâu 8 | 2.935 | 4.913 |
| Cây không giới hạn | 0.007 | 5.877 |

Linear được chọn; MAE test khoảng 3.637 phần. Đây chỉ là kết quả giáo dục
trên dữ liệu mô phỏng, không chứng minh độ chính xác cho nhà hàng thực.
Không sửa cấu hình dựa trên test rồi tiếp tục gọi đó là đánh giá độc lập.

## Đưa dữ liệu Hemi vào

```bash
python labs/Hemi.MachineLearning/lab.py --csv daily-sales.csv
```

CSV UTF-8 có header `date,menu_item_id,quantity`; ngày dạng `YYYY-MM-DD`.
Mỗi món có đúng một dòng mỗi ngày, quantity không âm. Cần ít nhất 88 ngày
liên tục (28 ngày warm-up + 60 ngày dùng được); thực tế nên có lịch sử dài hơn.

Xuất dữ liệu bằng cách join `FnbServiceOrder` với `FnbOrderLine`, lọc đơn
`Completed`, nhóm theo ngày nghiệp vụ tại UTC+7 và `MenuItemId`, cộng `Quantity`.
Chọn khoảng lịch sử đã hoàn tất và danh sách món ổn định; tạo đầy đủ lưới
ngày × món. Chỉ điền 0 khi xác nhận thực sự không bán; không điền 0 cho ngày
thiếu dữ liệu, nhà hàng đóng cửa hoặc món chưa được bán. Dataset không cần
tên khách hay số điện thoại. Kiểm tra ý nghĩa đơn hủy/hoàn và hết hàng trước
khi dùng: lượng bán quan sát được có thể thấp hơn nhu cầu thực.

Đặc trưng được tính trước khi thêm nhãn ngày hiện tại vào lịch sử.
Validation/test mô phỏng dự đoán cuốn chiếu từng ngày: khi dự đoán ngày D,
đã biết doanh số thực đến D−1, kể cả các ngày trước đó thuộc validation/test.
Đây không phải dự đoán toàn bộ nhiều tháng tại một thời điểm.
Không chuẩn hóa, mã hóa hay fit mô hình trên toàn bộ dataset trước khi chia.

## Tự kiểm tra đã hiểu

- Vì sao cây học gần hoàn hảo trên train lại thua hồi quy trên validation?
- Vì sao không shuffle ngẫu nhiên các dòng khi dự đoán tương lai?
- Vì sao dùng doanh số của chính ngày D trong X là leakage?
- Nếu baseline thắng ML trên validation, nên chọn gì và vì sao?

Tests kiểm tra thứ tự tập, đặc trưng chỉ dùng quá khứ, baseline cùng thứ,
và nhãn test không ảnh hưởng lựa chọn mô hình. Chưa tích hợp inference
vào ứng dụng; chỉ nên làm bước đó sau khi đánh giá trên dữ liệu thật.
