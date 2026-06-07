WITH TRUE_PREDICTIONS AS (
    SELECT
        *
    FROM READ_PARQUET('data/real_data-20260606T163916Z-3-001/real_data/batches/labels/{{ tables.name }}_Ground_Truth.parquet')
),
MODEL_PREDICTION AS (
    SELECT
        *
    FROM READ_PARQUET('data/predictions/{{ tables.name }}/ml_prediction_chunk_00000.parquet') 
),
SERVING_FEATURES AS (
    SELECT
        *
    FROM READ_PARQUET('data/real_data-20260606T163916Z-3-001/real_data/batches/features/{{ tables.name }}_features.parquet')    
)
SELECT  
    F.LABEL,
    T.PREDICTION,
    J.*
FROM TRUE_PREDICTIONS AS F
LEFT JOIN MODEL_PREDICTION AS T
    ON F.MERCHANT_ID = T.MERCHANT_ID
LEFT JOIN SERVING_FEATURES as J
    ON F.MERCHANT_ID = J.MERCHANT_ID

    