WITH FEATURES AS (
    SELECT
        ROW_NUMBER() OVER () AS ROW_ID,
        *
    FROM READ_PARQUET('data/real_data-20260606T163916Z-3-001/real_data/train/features_train.parquet')
),
TARGETS AS (
    SELECT
        ROW_NUMBER() OVER () AS ROW_ID,
        LABEL
    FROM READ_PARQUET('data/real_data-20260606T163916Z-3-001/real_data/train/target_train.parquet')
)
SELECT
    F.*,
    T.LABEL
FROM FEATURES AS F
JOIN TARGETS AS T 
    ON F.ROW_ID = T.ROW_ID