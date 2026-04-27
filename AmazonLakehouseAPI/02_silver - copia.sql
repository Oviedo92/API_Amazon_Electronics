CREATE SCHEMA silver;


--🧼 CLEAN LAYER: reseñas de productos (electronics)
CREATE OR REPLACE VIEW silver.vw_clean_electronics AS
SELECT

    -- 👤 quién opina (usuario)
    reviewerID,

    -- 👤 nombre del usuario (opcional, puede venir nulo)
    reviewerName,

    -- 📦 qué producto se está evaluando (clave para unir con metadata)
    asin,

    -- ⭐ qué calificación dio el usuario (rating de 1 a 5)
    overall,

    -- 💬 qué dijo el usuario (comentario completo)
    reviewText,

    -- 📝 resumen corto de la reseña
    summary,

    -- 📅 cuándo se hizo la reseña (timestamp en formato UNIX)
    unixReviewTime,

    -- 👍 votos útiles (puede venir como texto con comas)
    vote,

    -- ✅ si la compra fue verificada
    verified,

    -- 🎨 atributos adicionales del producto (ej: formato)
    style

FROM bronze.electronics
-- 🚫 eliminamos registros sin fecha (no útiles para análisis temporal)
WHERE unixReviewTime IS NOT NULL;


--⚙️ PROCESSED LAYER
CREATE OR REPLACE VIEW silver.vw_processed_electronics AS
SELECT

    -- ID estable
    HASH(reviewerID || '-' || asin || '-' || unixReviewTime) AS id_review,

    reviewerID AS id_usuario,
    asin AS id_producto,

    CAST(STRFTIME(TO_TIMESTAMP(unixReviewTime), '%Y%m%d') AS INTEGER) AS date_key,

    overall AS calificacion,

    TO_TIMESTAMP(unixReviewTime) AS fecha_completa,

    EXTRACT(YEAR FROM TO_TIMESTAMP(unixReviewTime)) AS anio,
    EXTRACT(MONTH FROM TO_TIMESTAMP(unixReviewTime)) AS mes,
    EXTRACT(DAY FROM TO_TIMESTAMP(unixReviewTime)) AS dia,
    EXTRACT(HOUR FROM TO_TIMESTAMP(unixReviewTime)) AS hora,
--
    EXTRACT(QUARTER FROM TO_TIMESTAMP(unixReviewTime)) AS trimestre,
    STRFTIME(TO_TIMESTAMP(unixReviewTime), '%W') AS semana,

    COALESCE(TRY_CAST(REPLACE(vote, ',', '') AS INTEGER), 0) AS votos,

    CASE WHEN verified = TRUE THEN 1 ELSE 0 END AS compra_verificada,

    reviewText AS comentario,
    summary AS resumen

FROM silver.vw_clean_electronics;



--🔥 ENRICHED LAYER
CREATE OR REPLACE VIEW silver.vw_enriched_electronics AS
SELECT

    r.*,

    -- producto info (JOIN lógico)
    p.nombre_producto,
    p.marca,
    p.categoria_principal,
    p.precio,

    -- feature engineering
    CASE 
        WHEN p.precio IS NULL THEN 'desconocido'
        WHEN p.precio < 5 THEN 'muy barato'
        WHEN p.precio < 20 THEN 'barato'
        WHEN p.precio < 100 THEN 'caro'
        ELSE 'muy caro'
    END AS segmento_precio,

    CASE 
        WHEN r.calificacion >= 4 THEN 'positivo'
        WHEN r.calificacion = 3 THEN 'neutral'
        ELSE 'negativo'
    END AS detalle_calificacion

FROM silver.vw_processed_electronics r
LEFT JOIN silver.vw_enriched_meta_electronics  p
ON r.id_producto = p.asin;

-------------------------------------------------------------------

--🧼 CLEAN LAYER: catálogo de productos
CREATE OR REPLACE VIEW silver.vw_clean_meta_electronics AS

SELECT
    -- 📦 identificador único del producto
    asin,

    -- 🏷️ nombre del producto
    title,

    -- 🏢 marca
    brand,

    -- 
    main_cat,

    -- 🧭 categorías (array)
    category,

    -- 📝 descripción (puede ser array/texto)
    description,

    -- ⭐ características del producto
    feature,

    -- 📅 fecha del producto (string original)
    date,

    -- 💲 precio en formato texto
    price

FROM bronze.meta_electronics;


--⚙️ PROCESSED LAYER: productos normalizados
CREATE OR REPLACE VIEW silver.vw_processed_meta_electronics AS
SELECT

    -- 🔑 ID producto
    asin,

    -- 🏷️ texto limpio
    TRIM(title) AS nombre_producto,
    TRIM(brand) AS marca,

    -- 📂 categorías
    category[1] AS categoria_principal,
    main_cat AS categoria_general,

    -- 📅 fecha convertida (string → date)
    TRY_STRPTIME(date, '%B %d, %Y') AS fecha_producto,

    -- 💲 precio limpio (string → double)
    TRY_CAST(REPLACE(price, '$', '') AS DOUBLE) AS precio,

    -- ⭐ arrays mantenidos
    feature AS caracteristicas,
    description AS descripcion,

    -- 🧠 métricas técnicas útiles
    array_length(feature) AS num_features,
    array_length(description) AS num_descripcion

FROM silver.vw_clean_meta_electronics;

--🔥 ENRICHED LAYER: calidad + segmentación del producto
CREATE OR REPLACE VIEW silver.vw_enriched_meta_electronics AS
SELECT
    asin,
    nombre_producto,
    marca,
    categoria_principal,
    categoria_general,
    fecha_producto,
    precio,
    caracteristicas,

    -- 📊 estado de calidad (data quality flags)
    CASE 
        WHEN fecha_producto IS NULL THEN 'desconocido'
        ELSE 'con fecha'
    END AS estado_fecha,

    CASE 
        WHEN precio IS NULL THEN 'desconocido'
        ELSE 'con precio'
    END AS estado_precio,

    CASE 
        WHEN caracteristicas IS NULL 
             OR array_length(caracteristicas) = 0 
        THEN 'desconocido'
        ELSE 'con features'
    END AS estado_caracteristicas,

    -- ⭐ score de calidad (0 a 4)
    (
        CASE WHEN fecha_producto IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN precio IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN caracteristicas IS NOT NULL 
                  AND array_length(caracteristicas) > 0 THEN 1 ELSE 0 END +
        CASE WHEN marca IS NOT NULL THEN 1 ELSE 0 END
    ) AS quality_score,

    -- 💰 segmentación de precio
-- 💰 segmentación de precio (realista)
        CASE 
            WHEN precio IS NULL THEN 'desconocido'
            WHEN precio < 10 THEN 'accesorio económico'
            WHEN precio < 50 THEN 'gadget básico'
            WHEN precio < 200 THEN 'electrónica media'
            WHEN precio < 500 THEN 'electrónica avanzada'
            ELSE 'premium'
        END AS segmento_precio,

        -- posicion del mercado
        CASE 
            WHEN precio IS NULL THEN 'desconocido'
            WHEN precio < 50 THEN 'entry level'
            WHEN precio < 300 THEN 'mid market'
                ELSE 'high end'
        END AS posicion_mercado
FROM silver.vw_processed_meta_electronics;