SELECT
*
FROM READ_PARQUET('data/real_data-20260606T163916Z-3-001/real_data/batches/features/{{ tables.name }}_features.parquet')