# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:**
**Repo:**
**Commit bài nộp:**
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** OpenAI Codex hỗ trợ đọc log, đối chiếu seed và ghi nhận CP0; chưa sửa logic pipeline. Học viên cần review và giải thích nội dung.
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Silver có 24 hàng / 12 ticket; T-91 giữ cả 3 trạng thái thay vì chỉ `high / closed / bug`. | u05 ngày 2026-08-12 có `(2 events, 1 click, 0 feedback down)`, kỳ vọng `(5, 3, 1)`; feature khác full recompute. | T-97 còn 2 hàng chưa xoá và dữ liệu cá nhân; snapshot mới nhất còn 1 hàng, RAG còn 2 chunks. |
| **Nguyên nhân gốc** | | | |
| **Cách sửa** (file, vài dòng) | | | |
| **Khái niệm trên slide** | | | |

## 2. Các con số

- Lateness từ **43 bản ghi Bronze**, theo ngày lịch: **P50 = 0,00; P95 = 2,90; P99 = 3,00; max = 3 ngày**. Baseline có `LOOKBACK_DAYS = 0`; số đo yêu cầu ít nhất `ceil(P99) = 3` ngày (chưa sửa ở CP0).
- `submission/checksums.txt`: **FAIL** — Gold fresh build: `f90edc98a9c0d5c6f5ef168dd422fe88`; cả ba rerun 2026-08-12 khác fresh build.
- dbt/parity: chưa chạy trong bằng chứng CP0.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`:
- Tombstone thay vì xoá hẳn hàng trong Silver:
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark:

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

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
