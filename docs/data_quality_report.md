
# Data Quality Report

## Total Records
942

## Total Columns
7

## Unique Counts
- Unique tickers: 59
- Unique persons: 893

## Null Summary

| Column | Null Count | Null Rate |
|--------|------------|-----------|
| ticker | 0.0 | 0.00% |
| exchange | 0.0 | 0.00% |
| person_name | 0.0 | 0.00% |
| role | 0.0 | 0.00% |
| source_agreement | 0.0 | 0.00% |
| confidence_score | 0.0 | 0.00% |
| merged_at | 0.0 | 0.00% |

## Source Agreement Distribution

| Category | Count |
|----------|-------|
| conflict | 413 |
| vietstock_only | 286 |
| cafef_only | 154 |
| both | 89 |

## Matching Metrics

- Match rate (both sources): 9.45%
- Conflict rate: 43.84%

## Unmatched Names

### CafeF Only (Top 10)

| Name | Count |
|------|-------|
| Ông Nguyễn Anh Tuấn | 3 |
| Ông Nguyễn Thanh Toại | 1 |
| Bà Phạm Thị Thanh Nga | 1 |
| Bà Phan Lạc Kim Trinh | 1 |
| Ông Từ Quốc Học | 1 |
| Ông Đặng Xuân Thắng | 1 |
| Ông Quách Kiều Hưng | 1 |
| Bà Lê Thị Hồng Nhung | 1 |
| Bà Trần Thị Nhung Gấm | 1 |
| Ông Phạm Hồng Hà | 1 |

### Vietstock Only (Top 10)

| Name | Count |
|------|-------|
| Ông Nguyễn Anh Tuấn | 3 |
| Ông Nguyễn Duy Khánh | 2 |
| Ông Nguyễn Việt Thắng | 2 |
| Ông Nguyễn Ngọc Quang | 2 |
| *** *** | 2 |
| Bà Bùi Thu Hà | 1 |
| Ông Hoàng Xuân Quốc | 1 |
| Ông Lê Chiến Thắng | 1 |
| Ông Lê Cự Tân | 1 |
| Ông Nguyễn Tuấn Hùng | 1 |

## Observations

- The match rate indicates partial overlap between CafeF and Vietstock.
- The conflict rate suggests differences in role naming conventions or update timing.
- Vietstock contains more single-source records than CafeF.
- Name normalization significantly improves matching quality.
- Further improvements could include fuzzy matching for higher alignment.
