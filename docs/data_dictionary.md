
# Data Dictionary

| Column | Data Type | Description |
|--------|-----------|-------------|
| ticker | object | Stock ticker symbol of the listed company. |
| exchange | object | Stock exchange where the company is listed (HOSE, HNX, UPCOM). |
| person_name | object | Full name of the board member as displayed on source. |
| role | object | Board/leadership role in Vietnamese. |
| source_agreement | object | Indicates whether the record appears in both sources, only one source, or contains conflicting information. |
| confidence_score | float64 | Confidence level assigned based on source agreement (1.0=both, 0.8=conflict, 0.6=single source). |
| merged_at | object | Timestamp indicating when the merge pipeline was executed. |
