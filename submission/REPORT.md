# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Nguyễn Quang Huy / 2A202602820
**Repo:** https://github.com/hshdhu/K4-Track02-Day17-NguyenQuangHuy-2A202602820-DataPipelineEngineering
**Commit mã nguồn dùng để kiểm tra:** `11bea068a8c89edc135f9981d1f1e849005e15b5` (CP3; CP4 chỉ cập nhật báo cáo, không đổi mã nguồn).
**AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex hỗ trợ đọc code/log, sửa ba lỗi, chạy kiểm tra, đối chiếu dbt và soạn báo cáo. Học viên chịu trách nhiệm review và giải thích thay đổi.
**Nguồn tham khảo:** README và docs RULES, RUBRIC, CHECKPOINTS, SUBMISSION trong repo; hướng dẫn lab do học viên cung cấp.

Phân tích mục 1–4 được rút gọn theo giới hạn một trang; output mục 5 không tính vào giới hạn. Định dạng tham chiếu: [SUBMISSION.md](../docs/SUBMISSION.md).

## 1. Ba lỗi

**Silver — một ticket, một trạng thái**
- Triệu chứng: 24 hàng/12 ticket; T-91 giữ ba trạng thái thay vì `high / closed / bug`.
- Nguyên nhân: dedup trong batch nhưng INSERT thêm hàng qua nhiều batch.
- Sửa: `pipeline/silver.py::upsert_silver_tickets` MERGE theo `ticket_id`; update khi LSN nguồn lớn hơn đích, insert khoá mới.
- Khái niệm: keyed upsert, LSN guard, idempotency; replay cũ không ghi đè trạng thái mới.

**Late data — feature theo event time**
- Triệu chứng: u05 ngày 08-12 có `(2 events, 1 click, 0 down)`, kỳ vọng `(5, 3, 1)`; feature lệch full recompute.
- Nguyên nhân: lookback 0 bỏ partition 08-12 khi events đến ngày 08-15.
- Sửa: `pipeline/config.py` đặt `LOOKBACK_DAYS=3`; Gold đã overwrite cửa sổ và nhóm theo ngày `event_time`.
- Khái niệm: event time khác ingest time; tính lại partition nhận sự kiện muộn đúng ngày.

**CDC delete — xoá truyền xuống Gold**
- Triệu chứng: T-97 còn PII trong Silver, một hàng ở snapshot mới nhất và hai RAG chunks.
- Nguyên nhân: lấy khoá từ `after=null` nên parser loại mất delete.
- Sửa: `pipeline/staging.py` coalesce khoá từ key/after/before; giữ op/LSN, đọc nội dung từ after để PII null. Silver/Gold hiện có áp dụng tombstone và loại ticket.
- Khái niệm: CDC delete mang LSN khác Kafka tombstone `value=null`; giữ LSN chống hồi sinh khi replay.

## 2. Các con số

- 43 bản ghi Bronze: P50=0, P95=2,90, P99=3,00, max=3 ngày; chọn lookback `ceil(P99)=3`, làm tròn lên tránh hụt cửa sổ.
- Verify **18/18**, pytest **34 passed**, dbt **PASS=19**, parity **PARITY** cho hai bảng chung.
- Rerun **PASS**: `C0=C1=C2=C3=39e115c510ecdf526800eac227158a4f`; [checksums.txt](checksums.txt) sinh tự động.

## 3. Lựa chọn kỹ thuật

- MERGE giữ trạng thái theo khoá/LSN; overwrite-partition tính tổng từ Silver, tránh cộng trùng khi rerun.
- Tombstone giữ khoá/LSN chống hồi sinh; user/subject/body đã null.
- Snapshot từ Bronze “as of” tái lập dữ liệu đã biết khi huấn luyện; snapshot mới nhận thay đổi, snapshot cũ giữ nguyên.
- DuckDB hợp dữ liệu nhỏ/cục bộ; dbt bổ sung model, contract và tests, chưa cần vận hành Spark.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến và xoá dữ liệu:** lab giữ snapshot cũ, loại T-97 khỏi snapshot mới nhất/RAG. Production cần thu hồi phiên bản chứa PII, dựng bản sạch và xoá có kiểm toán trên Bronze, snapshot, cache, bản sao/backup; đánh giá huấn luyện lại. Bất biến không cho phép giữ PII vô hạn; lab chưa chứng minh xoá PII đầy đủ.
2. **Regex còn bỏ sót tên người:** đặt nhận diện PII bằng regex kết hợp NER/ngữ cảnh tại Bronze→Silver, kiểm tra lại trước Gold/training/RAG; che tên bằng token, quarantine trường hợp không chắc chắn. Đo precision/recall theo loại PII trên bộ gán nhãn tiếng Việt, tỷ lệ rò rỉ và che nhầm; ưu tiên recall, review mẫu lỗi. Hạn chế quyền truy cập Bronze thô.

## 5. Output (dán nguyên văn)

### CP4 — Đối chiếu dbt

Đã đọc project/profile và các model staging/silver/gold. Profile dùng DuckDB cục bộ `dbt.duckdb`, một thread; staging là view, Silver/Gold incremental. dbt đọc cùng Bronze, giữ delete qua khoá trong before, che PII và loại event sai contract. Parity chỉ đối chiếu hai bảng chung, không chứng minh dbt triển khai toàn bộ pipeline Python.

- `unique_key='ticket_id'`: khoá khớp hàng trong incremental MERGE; dedup theo LSN chọn một thay đổi mới nhất mỗi ticket trước khi ghi.
- `merge_update_condition`: chỉ cập nhật khi LSN nguồn lớn hơn đích, tránh replay cũ ghi đè trạng thái mới; model còn lọc incremental bằng high-water mark LSN.
- `batch_size='day'`: feature chia thành microbatch một ngày theo event time, không theo ngày ingest.
- `lookback=3`: chạy incremental xét lại ba batch trước để nhận late data, khớp `ceil(P99)=3` đo từ Bronze.

Land Bronze xác nhận cả 21 batch nguồn/ngày đã tồn tại. Build dùng khoảng event time `[2026-08-10, 2026-08-17)`, nên đủ đến hết 08-16. Không sửa model hoặc checker: build và parity đã pass với cấu hình hiện có. Parity dựng lại Python trước khi so sánh. Output dbt dưới đây bỏ mã màu terminal, giữ nội dung; giờ là timestamp nguyên gốc của log.

```powershell
.\.venv\Scripts\python.exe main.py --land-only
$env:DO_NOT_TRACK = '1'
Push-Location dbt_project
try {
    ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
} finally {
    Pop-Location
}
```

```text
17:31:57  Running with dbt=1.12.5
17:31:58  Registered adapter: duckdb=1.11.0
17:31:58  Unable to do partial parsing because saved manifest not found. Starting full parse.
17:32:03  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
17:32:03
17:32:03  Concurrency: 1 threads (target='dev')
17:32:03
17:32:06  1 of 19 START sql view model main.stg_events ................................... [RUN]
17:32:06  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.22s]
17:32:06  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
17:32:07  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.06s]
17:32:07  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
17:32:07  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.14s]
17:32:07  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
17:32:07  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.24s]
17:32:07  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
17:32:07  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.11s]
17:32:07  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
17:32:07  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.06s]
17:32:07  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
17:32:07  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
17:32:07  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
17:32:07  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
17:32:07  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
17:32:07  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
17:32:07  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
17:32:07  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
17:32:07  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
17:32:07  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.05s]
17:32:07  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
17:32:07  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.05s]
17:32:07  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
17:32:07  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
17:32:07  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
17:32:07  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
17:32:07  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
17:32:07  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
17:32:07  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
17:32:07  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
17:32:08  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.07s]
17:32:08  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
17:32:08  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.09s]
17:32:08  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
17:32:08  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.05s]
17:32:08  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
17:32:08  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.08s]
17:32:08  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
17:32:08  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.04s]
17:32:08  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
17:32:08  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.06s]
17:32:08  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
17:32:08  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.07s]
17:32:08  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.52s]
17:32:08  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
17:32:08  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
17:32:08  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
17:32:08  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.03s]
17:32:08  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
17:32:08  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.03s]
17:32:08
17:32:08  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 5.33 seconds (5.33s).
17:32:08
17:32:08  Completed successfully
17:32:08
17:32:08  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```


### CP3 — Xoá truyền xuống Gold và rerun ổn định

CDC delete T-97 có khoá ở `key.ticket_id` và `before.ticket_id`, LSN `24020000`, `op=d`; Kafka tombstone kế tiếp có `value=null`, không mang thay đổi CDC/LSN và vẫn được giữ ở Bronze. Chỉ phục hồi khoá cho delete, không phục hồi PII từ before. Gold đã lọc delete khi dựng snapshot và dựng lại toàn bộ chunks từ ticket sống; không cần sửa `gold.py`. Cache embedding vẫn có thể giữ vector cũ, không coi đây là cơ chế xoá PII production.

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.84s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

Kiểm tra thêm trên DuckDB in-memory: xử lý bảy batch tickets rồi replay 08-12; assert T-97 vẫn giữ tombstone và LSN delete:

```text
T-97 after replay 2026-08-12: [(True, None, None, None, 24020000)]
```

### CP2 — Feature theo ngày xảy ra sự kiện

`event_time` là lúc sự kiện xảy ra; `_ingested_at` là lúc bản ghi đến Bronze. Ba event u05 xảy ra 08-12, đến 08-15 vẫn phải thuộc `event_date=2026-08-12`. Lookback 3 ngày cho cửa sổ bao gồm hai đầu `[08-12, 08-15]` khi chạy ngày 08-15. P99 và max đều là 3 trong seed này; dữ liệu thực trễ vượt cửa sổ cần backfill và theo dõi lateness lại.

```text
$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ .\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -k "feature_daily or late_events or lookback"
...                                                                      [100%]
3 passed, 10 deselected in 1.26s
```

Kiểm tra trực tiếp trên DuckDB in-memory: đọc từng batch Bronze, upsert events và tính feature theo ngày; assert kết quả u05 và checksum bằng full recompute:

```text
u05 / 2026-08-12 (events, clicks, feedback down): [(5, 3, 1)]
Feature checksum: 8630e04a61d10aa963b7a49e148926b0
Full recompute checksum: 8630e04a61d10aa963b7a49e148926b0
```

### CP1 — Một ticket, một trạng thái hiện tại

Hai tests yêu cầu đã pass. Kiểm tra thêm trên DuckDB in-memory đọc Bronze hiện có: Silver có 12 hàng / 12 ticket; T-91 là `high / closed / bug`, LSN `24024000`. Sau khi xử lý đủ bảy ngày, replay lần lượt 08-12, 08-14, 08-16, 08-12 không thay đổi bất kỳ cột nào của toàn bộ bảng ticket. `_lsn` lấy từ `value.source.lsn` là thứ tự CDC để quyết định update, không dùng ngày chạy batch. Cần fresh build để dựng lại warehouse baseline vốn đã có hàng trùng; thay đổi này bảo đảm các lần ghi vào bảng sạch duy trì một hàng mỗi khoá. Late data/delete/chunks sẽ xử lý ở checkpoint sau; checksum mục 2 là bằng chứng CP0.

Lần chạy pytest đầu bị sandbox chặn quyền thư mục tạm; output dưới đây là lần chạy lại thành công với quyền cần thiết.

```text
$ .\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -k "one_row_per_ticket or latest_state_wins"
..                                                                       [100%]
2 passed, 11 deselected in 1.19s
```

Output kiểm tra replay trực tiếp (assert toàn bộ hàng/cột trước và sau bằng nhau):

```text
Silver rows / distinct tickets: (12, 12)
T-91 priority / status / category / LSN: [('high', 'closed', 'bug', 24024000)]
Replay older/equal LSN batches: PASS (all Silver ticket columns unchanged)
```

### CP0 — Ghi nhận baseline trước khi sửa

Nguồn bằng chứng: log PowerShell học viên cung cấp, dán bên dưới. Fresh build xử lý đủ 7 ngày seed; verify **8/18**, pytest **25 passed / 9 failed**. Các lệnh trong log không gặp lỗi thiếu module hay file; lỗi contract của baseline là chủ đích đề bài.

Ba nguồn: CDC tickets, Kafka events và transcript JSON. Đường đi: `data/ → Bronze Parquet → Silver DuckDB → Gold DuckDB`. Bronze giữ dữ liệu thô để replay, Silver chuẩn hoá/kiểm chất lượng, Gold phục vụ feature, training snapshot và RAG.

- **T-91:** tạo ngày 08-10 ở `low / open / null`; ngày 08-14 thành `high / open / null` (có CDC giao lại); ngày 08-16 thành `high / closed / bug`. Silver baseline giữ cả ba trạng thái.
- **T-97:** tạo ngày 08-11, đóng ngày 08-12; ngày 08-15 nhận CDC delete `op=d, after=null`, rồi Kafka tombstone `value=null`. Baseline chưa lưu trạng thái xoá ở Silver và chưa truyền xoá đến snapshot mới nhất/RAG.
- **u05:** `e-0017`, `e-0018`, `e-0019` xảy ra ngày 08-12 nhưng đến trong batch 08-15, trễ 3 ngày; gồm 2 clicks và 1 feedback down. Feature ngày 08-12 chưa cộng chúng.

Các check verify fail: ticket duy nhất; trạng thái cuối T-91; tombstone T-97; feature khớp full recompute; late events u05; lookback bao phủ P99; loại T-97 khỏi snapshot mới nhất; loại T-97 khỏi RAG; chunks duy nhất/rerun; checksum Gold ổn định khi rerun. Chunks baseline có **22 hàng / 9 khoá chunk**; rerun embed 0 chunks mới vẫn đổi checksum. Các mục nguyên nhân/cách sửa và suy ngẫm sẽ bổ sung ở checkpoint sau.

### Output baseline PowerShell

```text
(.venv) PS C:\Users\Huy\P-051\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe main.py
=== Day 17 pipeline: fresh build 2026-08-10 .. 2026-08-16 ===
  2026-08-10  bronze+12  tickets:4   events:6   quarantined:0 features[2026-08-10..2026-08-10] snapshot v2026-08-10:1 chunks:2 (embedded 2)
  2026-08-11  bronze+10  tickets:7   events:11  quarantined:0 features[2026-08-11..2026-08-11] snapshot v2026-08-11:2 chunks:5 (embedded 2)
  2026-08-12  bronze+12  tickets:12  events:17  quarantined:0 features[2026-08-12..2026-08-12] snapshot v2026-08-12:5 chunks:8 (embedded 1)
  2026-08-13  bronze+11  tickets:15  events:21  quarantined:1 features[2026-08-13..2026-08-13] snapshot v2026-08-13:6 chunks:10 (embedded 1)
  2026-08-14  bronze+9   tickets:18  events:25  quarantined:0 features[2026-08-14..2026-08-14] snapshot v2026-08-14:7 chunks:12 (embedded 1)
  2026-08-15  bronze+13  tickets:20  events:32  quarantined:1 features[2026-08-15..2026-08-15] snapshot v2026-08-15:8 chunks:15 (embedded 1)
  2026-08-16  bronze+13  tickets:24  events:39  quarantined:0 features[2026-08-16..2026-08-16] snapshot v2026-08-16:10 chunks:22 (embedded 3)

Gold checksums:
  gold_feature_daily   c50b8851affeb418fcb824e65099d9be
  gold_training_set    bd80ed585cda944fc9f9d63a63f00316
  gold_doc_chunks      b49150795e84b6016aa19297b2707a03
  gold (combined)      f90edc98a9c0d5c6f5ef168dd422fe88
(.venv) PS C:\Users\Huy\P-051\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [XX ] Silver  silver_tickets has exactly one row per ticket_id  (24 rows for 12 tickets)
  [XX ] Silver  T-91 shows its latest state: high / closed / bug  (got [('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')])
  [XX ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left  (got [(False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.'), (False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.')])
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [XX ] Gold    gold_feature_daily reconciles with a full recompute from Silver  (c50b8851affe != 8630e04a61d1)
  [XX ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12  (got (2, 0), expected (5, 1))
  [XX ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)  (LOOKBACK_DAYS=0 < 3)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [XX ] Gold    latest training snapshot excludes the deleted ticket T-97  (1 row(s))
  [XX ] Gold    deletes propagate to the RAG index: no chunk of T-97  (2 chunk(s))
  [XX ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks  (22 rows / 9chunks, embedded 0)
  [XX ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build  (see submission/checksums.txt)

RESULT: 8/18 checks — FAILURES ABOVE
re-run checksums written to submission/checksums.txt
(.venv) PS C:\Users\Huy\P-051\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m pytest
.FFF..FFF..FF...........F.........                                                          [100%]
============================================ FAILURES ============================================
_____________________________ test_silver_tickets_one_row_per_ticket _____________________________

built = (<_duckdb.DuckDBPyConnection object at 0x00000155BE4148B0>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_silver_tickets_one_row_per_ticket(built):
        con, _ = built
        n, n_ids = con.execute("SELECT count(*), count(DISTINCT ticket_id) FROM silver_tickets").fetchone()
>       assert n == n_ids == 12
E       assert 24 == 12

tests\test_contracts.py:34: AssertionError
_____________________________ test_silver_tickets_latest_state_wins ______________________________

built = (<_duckdb.DuckDBPyConnection object at 0x00000155BE4148B0>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_silver_tickets_latest_state_wins(built):
        con, _ = built
>       assert rows(con, "SELECT priority, status, category FROM silver_tickets "
                         "WHERE ticket_id = 'T-91'") == [("high", "closed", "bug")]
E       AssertionError: assert [('low', 'ope...osed', 'bug')] == [('high', 'closed', 'bug')]
E         
E         At index 0 diff: ('low', 'open', None) != ('high', 'closed', 'bug')
E         Left contains 2 more items, first extra item: ('high', 'open', None)
E         Use -v to get more diff

tests\test_contracts.py:39: AssertionError
_______________________________ test_cdc_delete_becomes_tombstone ________________________________

built = (<_duckdb.DuckDBPyConnection object at 0x00000155BE4148B0>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_cdc_delete_becomes_tombstone(built):
        con, _ = built
>       assert rows(con, "SELECT is_deleted, user_id, subject, body FROM silver_tickets "
                         "WHERE ticket_id = 'T-97'") == [(True, None, None, None)]
E       AssertionError: assert [(False, 'u06...ệu của tôi.')] == [(True, None, None, None)]
E         
E         At index 0 diff: (False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.') != (True, None, None, None)
E         Left contains one more item: (False, 'u06', 'Yêu cầu xoá ...
E         
E         ...Full output truncated (2 lines hidden), use '-vv' to show

tests\test_contracts.py:45: AssertionError
_______________________ test_feature_daily_reconciles_with_full_recompute ________________________

built = (<_duckdb.DuckDBPyConnection object at 0x00000155BE4148B0>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_feature_daily_reconciles_with_full_recompute(built):
        con, _ = built
>       assert query_checksum(con, "SELECT * FROM gold_feature_daily") == \
            query_checksum(con, feature_daily_full_recompute_sql())
E       AssertionError: assert 'c50b8851affe...824e65099d9be' == '8630e04a61d1...7a49e148926b0'
E         
E         - 8630e04a61d10aa963b7a49e148926b0
E         + c50b8851affeb418fcb824e65099d9be

tests\test_contracts.py:67: AssertionError
____________________________ test_late_events_land_in_their_event_day ____________________________

built = (<_duckdb.DuckDBPyConnection object at 0x00000155BE4148B0>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_late_events_land_in_their_event_day(built):
        con, _ = built
>       assert rows(con, """SELECT n_events, n_clicks, n_feedback_down FROM gold_feature_daily
                            WHERE user_id = 'u05' AND event_date = DATE '2026-08-12'""") == [(5, 3, 1)]
E       assert [(2, 1, 0)] == [(5, 3, 1)]
E         
E         At index 0 diff: (2, 1, 0) != (5, 3, 1)
E         Use -v to get more diff

tests\test_contracts.py:73: AssertionError
_____________________________ test_lookback_covers_measured_lateness _____________________________

built = (<_duckdb.DuckDBPyConnection object at 0x00000155BE4148B0>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_lookback_covers_measured_lateness(built):
        con, _ = built
>       assert config.LOOKBACK_DAYS >= lateness_profile(con)["p99_days"]
E       assert 0 >= 3
E        +  where 0 = config.LOOKBACK_DAYS

tests\test_contracts.py:79: AssertionError
__________________________ test_deleted_ticket_leaves_training_and_rag ___________________________

built = (<_duckdb.DuckDBPyConnection object at 0x00000155BE4148B0>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_deleted_ticket_leaves_training_and_rag(built):
        con, _ = built
>       assert rows(con, """SELECT count(*) FROM gold_training_set
                            WHERE snapshot_version = 'v2026-08-16' AND ticket_id = 'T-97'""") == [(0,)]
E       assert [(1,)] == [(0,)]
E         
E         At index 0 diff: (1,) != (0,)
E         Use -v to get more diff

tests\test_contracts.py:98: AssertionError
_____________________________________ test_doc_chunks_unique _____________________________________

built = (<_duckdb.DuckDBPyConnection object at 0x00000155BE4148B0>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_doc_chunks_unique(built):
        con, _ = built
        n, n_distinct = con.execute("""SELECT count(*), count(DISTINCT (ticket_id, chunk_idx))
                                       FROM gold_doc_chunks""").fetchone()
>       assert n == n_distinct == 8
E       assert 22 == 9

tests\test_contracts.py:107: AssertionError
_______________________ test_rerun_old_day_three_times_keeps_gold_checksum _______________________

sandbox = WindowsPath('C:/Users/Huy/AppData/Local/Temp/pytest-of-Huy/pytest-18/lab171')

    def test_rerun_old_day_three_times_keeps_gold_checksum(sandbox):
        from scripts.rerun_check import rerun_check
        res = rerun_check(config.RERUN_DAY, write=False, quiet=True)
>       assert res["ok"], res["runs"]
E       AssertionError: [('fresh build', {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chunks': 'b49150795e84b6016aa19297b2707a03', '...', 'gold_feature_daily': '21d1035eba8aaa7a60e7360302f33519', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})]
E       assert False

tests\test_rerun.py:15: AssertionError
==================================== short test summary info =====================================
FAILED tests/test_contracts.py::test_silver_tickets_one_row_per_ticket - assert 24 == 12
FAILED tests/test_contracts.py::test_silver_tickets_latest_state_wins - AssertionError: assert [('low', 'ope...osed', 'bug')] == [('high', 'closed', 'bug')]
FAILED tests/test_contracts.py::test_cdc_delete_becomes_tombstone - AssertionError: assert [(False, 'u06...ệu của tôi.')] == [(True, None, None, None)]
FAILED tests/test_contracts.py::test_feature_daily_reconciles_with_full_recompute - AssertionError: assert 'c50b8851affe...824e65099d9be' == '8630e04a61d1...7a49e148926b0'
FAILED tests/test_contracts.py::test_late_events_land_in_their_event_day - assert [(2, 1, 0)] == [(5, 3, 1)]
FAILED tests/test_contracts.py::test_lookback_covers_measured_lateness - assert 0 >= 3
FAILED tests/test_contracts.py::test_deleted_ticket_leaves_training_and_rag - assert [(1,)] == [(0,)]
FAILED tests/test_contracts.py::test_doc_chunks_unique - assert 22 == 9
FAILED tests/test_rerun.py::test_rerun_old_day_three_times_keeps_gold_checksum - AssertionError: [('fresh build', {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chunks...
9 failed, 25 passed in 2.51s
(.venv) PS C:\Users\Huy\P-051\K4-Track02-Day17-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 0
(.venv) PS C:\Users\Huy\P-051\K4-Track02-Day17-Data-Pipeline-Engineering>
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
