SELECT
    CAST(id AS INT64) AS product_id,
    COALESCE(NULLIF(CAST(sku AS STRING), ''), CAST(id AS STRING)) AS sku,
    name,
    title AS size,
    (CAST(net_price AS FLOAT64) / 100) AS net_price,
    (COALESCE(CAST(gross_price AS FLOAT64), CAST(net_price AS FLOAT64), 0.) / 100) AS gross_price,
    currency,
    CAST(barcode AS STRING) AS barcode,
    CAST(stock AS BOOLEAN) AS is_available,
    stock_quantity
FROM {{ source('raw_data', 'products') }}