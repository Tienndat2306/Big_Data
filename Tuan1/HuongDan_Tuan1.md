# Hướng dẫn chi tiết – Bài thực hành Big Data Tuần 1
**Apache Spark, RDD và DataFrame trên Google Colab (PySpark 3.5.1, dữ liệu NYC Yellow Taxi 01/2024)**

---

## 0. Cách dùng file notebook

1. Vào https://colab.research.google.com → **File → Upload notebook** → chọn `Tuan1_HoTen_MSSV.ipynb`.
2. Đổi tên file thành `Tuan1_<HoTen>_<MSSV>.ipynb` (đúng cú pháp đề yêu cầu).
3. Ở **Câu 1**, sửa `MSSV = "MSSV"` thành mã số sinh viên của bạn. Ở ô markdown đầu, điền họ tên và MSSV.
4. Chọn **Runtime → Run all**, hoặc chạy từng ô theo thứ tự từ trên xuống.
5. Khi mọi ô đã có kết quả: **File → Download → Download .ipynb** rồi nộp.

> **Quan trọng:** file `.ipynb` mình gửi chứa code và lời giải thích nhưng **chưa có kết quả chạy** (môi trường của mình không có internet để tải dữ liệu). Đề yêu cầu nộp notebook "hiển thị đầy đủ kết quả đầu ra", nên bạn **bắt buộc phải tự chạy trên Colab một lần** trước khi tải về nộp.

Nếu Colab ngắt phiên, file `taxi_data.parquet` và các biến sẽ mất, chỉ cần **Run all** lại từ đầu.

---

## 1. Kiến thức nền cần nắm

| Khái niệm | Ý nghĩa ngắn gọn |
|---|---|
| **SparkSession** | Cổng vào để làm việc với Spark (đọc dữ liệu, tạo DataFrame, truy cập `sparkContext`). |
| **local[\*]** | Chạy Spark trên máy hiện tại, dùng tất cả nhân CPU. |
| **DataFrame** | Bảng dữ liệu có cột và kiểu dữ liệu, xử lý phân tán. |
| **RDD** | Tập dữ liệu phân tán mức thấp hơn, thao tác bằng hàm `map`, `filter`, `reduceByKey`... |
| **Transformation** | Phép biến đổi, thực thi **lười** (chưa chạy ngay): `map`, `filter`, `reduceByKey`, `sortBy`. |
| **Action** | Hành động kích hoạt tính toán thật: `count`, `sum`, `mean`, `collect`, `show`. |
| **Parquet** | Định dạng cột nén, nhanh, lưu kèm schema. |

---

## 2. Chuẩn bị môi trường

```python
!pip install -q pyspark==3.5.1
!wget -q https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet -O taxi_data.parquet
```

- `pip install` cài PySpark. Colab có sẵn Java nên không cần cài thêm.
- `wget ... -O taxi_data.parquet` tải file về và **đặt tên là `taxi_data.parquet`** (khớp với đề bài).
- Mình thêm `!ls -lh taxi_data.parquet` để kiểm tra file đã tải (dung lượng khoảng vài chục MB).

**Lỗi thường gặp:** nếu `wget` không tải được (file rỗng/404), chạy lại ô, hoặc kiểm tra mạng của Colab. Nếu nhà cung cấp dữ liệu đổi đường dẫn, hỏi giảng viên link thay thế.

---

## Phần A: Khởi tạo và khám phá dữ liệu

### Câu 1: SparkSession, đọc Parquet, `printSchema()`, `show(5)`

**Ý tưởng:** tạo session có tên `ThucHanh_Tuan1_<MSSV>`, chạy `local[*]`, đọc Parquet bằng `spark.read.parquet`.

```python
MSSV = "MSSV"   # thay bằng MSSV của bạn
spark = (SparkSession.builder
         .appName(f"ThucHanh_Tuan1_{MSSV}")
         .master("local[*]")
         .getOrCreate())
df = spark.read.parquet("taxi_data.parquet")
df.printSchema()
df.show(5)
```

- `f"...{MSSV}"` là f-string, tự ghép MSSV vào tên ứng dụng.
- `printSchema()` in cấu trúc cột. Bạn sẽ thấy các cột trong bảng mô tả đề bài (`tpep_pickup_datetime`, `passenger_count`, `trip_distance`, `payment_type`, `fare_amount`, `tip_amount`, `total_amount`...) cùng nhiều cột khác.
- `show(5)` hiển thị 5 dòng đầu.

**Kết quả đúng:** thấy schema dạng cây và bảng 5 dòng.

---

### Câu 2: Đếm tổng số chuyến và số chuyến bất thường

```python
tong_chuyen = df.count()
so_bat_thuong = df.filter((col("trip_distance") <= 0) | (col("total_amount") <= 0)).count()
```

- `count()` là action trả về số dòng. Tập dữ liệu khoảng **2,96 triệu** chuyến.
- Điều kiện "**hoặc**" dùng toán tử `|`. Mỗi vế phải đặt trong ngoặc tròn, vì `|` có độ ưu tiên cao hơn `<=`.
- Mình in thêm tỷ lệ phần trăm để dễ nhận xét.

**Lỗi thường gặp:** viết `or` thay cho `|` sẽ báo lỗi "Cannot convert column into bool".

---

### Câu 3: Tạo `clean_df` và tính tỷ lệ bản ghi sạch

```python
clean_df = df.filter(
    (col("trip_distance") > 0) &
    (col("total_amount") > 0) &
    (col("passenger_count") > 0)
).cache()
so_sach = clean_df.count()
print(so_sach / tong_chuyen * 100)
```

- Dùng `&` cho điều kiện "**đồng thời**" (cả ba điều kiện phải đúng).
- Dòng có `passenger_count` bị `null` sẽ tự bị loại, vì phép so sánh với `null` không cho kết quả đúng.
- `.cache()` giữ `clean_df` trong bộ nhớ, giúp các câu 4–8 chạy nhanh hơn vì không phải lọc lại từ đầu.
- Tỷ lệ = `số bản ghi sạch / tổng ban đầu × 100`.

**Lưu ý khi viết nhận xét:** tỷ lệ sạch thường chiếm đa số, nhưng có một phần dữ liệu bị loại do quãng đường bằng 0, tổng tiền không dương, hoặc thiếu số khách.

---

## Phần B: Xử lý dữ liệu phân tán với RDD

### Câu 4: RDD quãng đường – tổng, trung bình, lớn nhất, nhỏ nhất

```python
distance_rdd = clean_df.select("trip_distance").rdd.map(lambda row: float(row[0]))
tong_qd = distance_rdd.sum()
tb_qd   = distance_rdd.mean()
max_qd  = distance_rdd.max()
min_qd  = distance_rdd.min()
```

- `select("trip_distance").rdd` chuyển DataFrame một cột sang RDD các `Row`.
- `.map(lambda row: float(row[0]))` lấy giá trị ra khỏi `Row`, thành RDD các số thực (đúng yêu cầu "RDD các số thực").
- RDD số có sẵn các phương thức `sum()`, `mean()`, `max()`, `min()`.

**Nhận xét gợi ý:** quãng đường dài nhất thường rất lớn (hàng chục nghìn dặm, nhiều khả năng là lỗi ghi nhận dù đã lọc `> 0`), còn ngắn nhất rất nhỏ (như 0.01 dặm). Bạn có thể ghi chú rằng dữ liệu vẫn còn giá trị ngoại lai (outlier).

---

### Câu 5: MapReduce đếm số chuyến theo số lượng hành khách

```python
passenger_rdd = clean_df.select("passenger_count").rdd.map(lambda row: int(row[0]))
ket_qua = (passenger_rdd
           .map(lambda p: (p, 1))
           .reduceByKey(add)
           .sortBy(lambda kv: kv[1], ascending=False))
for so_khach, so_chuyen in ket_qua.collect():
    print(so_khach, so_chuyen)
```

- **Map:** mỗi chuyến thành cặp `(số khách, 1)`.
- **Reduce:** `reduceByKey(add)` cộng các số 1 theo từng số khách, ra `(số khách, số chuyến)`.
- `passenger_count` kiểu Double nên chuyển bằng `int()` để in `1`, `2` thay vì `1.0`, `2.0`.
- `sortBy(..., ascending=False)` sắp xếp giảm dần theo số chuyến, đúng yêu cầu đề.
- `collect()` an toàn ở đây vì kết quả chỉ vài dòng (mức số khách rất ít).

**Nhận xét gợi ý:** chuyến 1 khách chiếm áp đảo, giảm dần ở 2, 3, 4 khách.

---

### Câu 6: Đếm chuyến có tiền boa từ $10 trở lên

```python
tip_rdd = clean_df.select("tip_amount").rdd.map(lambda row: row[0])
so_chuyen_boa_cao = tip_rdd.filter(lambda t: t is not None and t >= 10).count()
```

- `filter` giữ lại phần tử thỏa điều kiện `t >= 10` (**từ 10 trở lên**, nên dùng `>=`, không dùng `>`).
- `count()` là action đếm số phần tử còn lại.
- `t is not None` phòng khi có giá trị rỗng.

---

## Phần C: Phân tích, gom nhóm và xuất kết quả với DataFrame

### Câu 7: Thống kê theo hình thức thanh toán

```python
pay_df = (clean_df.groupBy("payment_type")
          .agg(F.count("*").alias("tong_chuyen"),
               F.round(F.avg("fare_amount"), 2).alias("cuoc_tb"),
               F.round(F.avg("tip_amount"), 2).alias("boa_tb"))
          .orderBy("payment_type"))
pay_df.show()
```

- `groupBy` gom nhóm theo mã thanh toán, `agg` tính nhiều chỉ số cùng lúc.
- `alias` đặt tên cột kết quả, `round(…, 2)` làm tròn 2 chữ số thập phân.

**Lời nhận xét (đã chép vào comment trong code):**
- Thẻ (1) có tiền boa trung bình cao, tiền mặt (2) gần như bằng 0.
- Nguyên nhân chính: `tip_amount` chỉ được hệ thống ghi tự động khi trả bằng thẻ. Tiền boa bằng tiền mặt đưa thẳng cho tài xế và không được nhập vào dữ liệu (đề bài cũng ghi chú điều này ở mô tả cột `tip_amount`).
- Do đó chênh lệch là do **cách ghi nhận dữ liệu**, không nên kết luận khách trả tiền mặt không boa.
- Có thể bổ sung: cước phí trung bình giữa hai nhóm khá gần nhau.

---

### Câu 8: Khung giờ cao điểm

```python
hour_df = (clean_df
           .withColumn("gio_don", hour(col("tpep_pickup_datetime")))
           .groupBy("gio_don")
           .agg(F.count("*").alias("tong_chuyen"),
                F.round(F.sum("total_amount"), 2).alias("tong_doanh_thu")))
hour_df.orderBy("gio_don").show(24)
top5_df = hour_df.orderBy(col("tong_chuyen").desc()).limit(5)
top5_df.show()
```

- `hour()` trích giờ (0–23) từ cột thời gian. `withColumn` thêm cột mới `gio_don`.
- Thống kê đủ 24 khung giờ (`show(24)` để hiện hết, mặc định `show()` chỉ in 20 dòng).
- Top 5: sắp xếp giảm dần theo số chuyến rồi `limit(5)`. Kết quả `top5_df` được dùng lại ở Câu 10.

**Nhận xét gợi ý:** nhóm giờ cao điểm thường rơi vào chiều tối (khoảng 15h–19h), còn sáng sớm (khoảng 3h–5h) rất thưa. Bạn hãy đối chiếu với bảng kết quả thực tế của mình rồi ghi nhận xét.

---

### Câu 9: Đo hiệu năng `toPandas().sum()` và `agg(sum)`

```python
t1 = time.time()
pdf = df.select("total_amount").toPandas()
tong_pandas = pdf["total_amount"].sum()
t2 = time.time()

t3 = time.time()
tong_spark = df.agg(spark_sum("total_amount")).collect()[0][0]
t4 = time.time()
```

- `time.time()` lấy mốc thời gian, hiệu hai mốc là số giây chạy.
- Phương pháp (1): kéo dữ liệu về RAM của driver (Pandas) rồi cộng.
- Phương pháp (2): Spark cộng trên từng partition, chỉ trả về **một con số**.
- Mình chỉ `select("total_amount")` trước khi `toPandas()` để tránh đầy RAM của Colab (chuyển cả ~19 cột × 3 triệu dòng có thể làm treo máy). Hai tổng phải **gần như bằng nhau**, đó là cách kiểm tra kết quả đúng.

**Lưu ý khi so sánh:**
- Thời gian thay đổi giữa các lần chạy. Hãy chạy ô này 2–3 lần và lấy kết quả ổn định.
- Lần chạy PySpark đầu có thể đã gồm thời gian đọc file Parquet, nên chênh lệch không phải lúc nào cũng nghiêng hẳn về một phía. Nếu kết quả thực tế của bạn khác với mong đợi (ví dụ Pandas nhanh hơn), cứ **báo cáo đúng số đo của bạn** rồi giải thích: với dữ liệu cỡ vài triệu dòng và chạy trên một máy, lợi thế của Spark không rõ rệt, nó chỉ thật sự nổi bật khi dữ liệu rất lớn hoặc có nhiều máy.
- Điểm chắc chắn: `toPandas()` đưa toàn bộ dữ liệu về một nơi, nên không mở rộng được với dữ liệu lớn, còn `agg` thì mở rộng được.

---

### Câu 10: Xuất CSV Top 5 khung giờ

```python
(top5_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv("/content/ketqua_tuan1"))
```

- `coalesce(1)` gộp thành 1 partition, nên chỉ ra **1 file** dữ liệu.
- `option("header", True)` ghi dòng tiêu đề.
- `mode("overwrite")` cho phép chạy lại mà không báo lỗi "đường dẫn đã tồn tại".
- Spark luôn tạo ra một **thư mục** `ketqua_tuan1` chứa file `part-00000-....csv` (kèm `_SUCCESS`). Đây là hành vi bình thường, không phải lỗi. Ô code đọc lại file bằng Pandas để bạn xác nhận nội dung.
- Muốn tải file về: bấm biểu tượng thư mục bên trái Colab → `ketqua_tuan1` → chọn file `part-...csv` → Download.

---

## 3. Lỗi thường gặp tổng hợp

| Hiện tượng | Nguyên nhân / cách xử lý |
|---|---|
| `NameError: name 'df' is not defined` | Chưa chạy Câu 1 (hoặc Colab bị reset). Chạy lại từ đầu. |
| `Cannot convert column into bool` | Dùng `and/or`; phải dùng `&`, `|` và bọc từng điều kiện bằng ngoặc. |
| `Path does not exist: taxi_data.parquet` | Tải file thất bại. Chạy lại ô tải dữ liệu, kiểm tra `!ls`. |
| Colab treo hoặc báo hết RAM ở Câu 9 | Chỉ `select` cột cần dùng trước khi `toPandas()`. |
| Chạy chậm | Bình thường với 3 triệu dòng; `cache()` đã hỗ trợ. Đợi ô chạy xong. |
| `Py4JJavaError` khi chạy lại sau nhiều lần | Chọn **Runtime → Restart session** rồi Run all. |

## 4. Danh sách kiểm tra trước khi nộp

- [ ] Đã sửa `MSSV` ở Câu 1 và điền họ tên đầu notebook.
- [ ] Đã chạy toàn bộ các ô, **mọi ô đều có kết quả đầu ra** (không có lỗi đỏ).
- [ ] Đã xem nhận xét ở Câu 7 (và các nhận xét thêm) có khớp số liệu thực tế của bạn.
- [ ] File CSV nằm ở `/content/ketqua_tuan1`.
- [ ] Tên file `Tuan1_HoTen_MSSV.ipynb` đúng cú pháp.
- [ ] Đã tải bằng **File → Download → Download .ipynb** và nộp lên hệ thống.
