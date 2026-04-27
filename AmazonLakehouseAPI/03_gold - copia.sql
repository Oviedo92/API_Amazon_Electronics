CREATE SCHEMA gold;


CREATE OR REPLACE TABLE gold.dim_tiempo AS
SELECT DISTINCT

    date_key,
    fecha_completa,
    anio,
    mes,
    dia,
    hora,
    semana,
    trimestre
FROM silver.vw_processed_electronics;



CREATE OR REPLACE TABLE gold.dim_usuario AS
SELECT DISTINCT

    id_usuario,

    -- nombre (puede venir nulo)
    MAX(reviewerName) AS nombre_usuario,

    -- métricas útiles 
    COUNT(*) AS total_reviews,
    AVG(calificacion) AS rating_promedio_usuario

FROM silver.vw_clean_electronics c
JOIN silver.vw_processed_electronics p
    ON c.reviewerID = p.id_usuario
GROUP BY id_usuario;



CREATE OR REPLACE TABLE gold.dim_producto AS
SELECT

    asin AS id_producto,
    nombre_producto,
    marca,

    categoria_principal,
    categoria_general,   -- ✅ agregado

    fecha_producto,
    precio,

    caracteristicas,     -- ✅ agregado

    estado_fecha,
    estado_precio,
    estado_caracteristicas,
    quality_score,

    segmento_precio,
    posicion_mercado
FROM silver.vw_enriched_meta_electronics;


CREATE OR REPLACE TABLE gold.fact_reseñas AS
SELECT

    id_review,
    id_usuario,
    id_producto,
    date_key,

    calificacion AS rating,
    votos,
    compra_verificada,

    -- 💬 texto (control de nulos)
    COALESCE(comentario, 'sin comentario') AS comentario,
    COALESCE(resumen, 'sin resumen') AS resumen,

    -- 📊 métricas derivadas
    CASE 
        WHEN calificacion >= 4 THEN 1 ELSE 0
    END AS es_positivo,

    CASE 
        WHEN calificacion = 3 THEN 1 ELSE 0
    END AS es_neutral,

    CASE 
        WHEN calificacion <= 2 THEN 1 ELSE 0
    END AS es_negativo
FROM silver.vw_enriched_electronics;



-- consulta optimizada
CREATE OR REPLACE TABLE gold.fact_reseñas AS
SELECT
    -- 🔑 claves
    id_review,
    id_usuario,
    id_producto,
    date_key,

    -- ⭐ comportamiento
    calificacion AS rating,
    votos,
    compra_verificada,

    -- 💬 texto limpio
    COALESCE(comentario, 'sin comentario') AS comentario,
    COALESCE(resumen, 'sin resumen') AS resumen,

    -- 🧠 métricas derivadas
    CASE WHEN calificacion >= 4 THEN 1 ELSE 0 END AS es_positivo,
    CASE WHEN calificacion = 3 THEN 1 ELSE 0 END AS es_neutral,
    CASE WHEN calificacion <= 2 THEN 1 ELSE 0 END AS es_negativo,

    -- 💰 valor de negocio (CLAVE)
    (calificacion * COALESCE(votos, 1)) AS valor_interaccion,

    -- ⚠️ riesgo del producto
    CASE 
        WHEN calificacion <= 2 THEN 1 ELSE 0
    END AS flag_riesgo,

    -- 📦 contribución a negocio (simple proxy)
    CASE 
        WHEN calificacion >= 4 AND votos > 20 THEN 'producto_valioso'
        WHEN calificacion <= 2 THEN 'producto_problematico'
        ELSE 'neutro'
    END AS impacto_negocio

FROM silver.vw_enriched_electronics;



-- consulta optimizada
CREATE OR REPLACE TABLE gold.dim_usuario AS
SELECT
    id_usuario,

    MAX(reviewerName) AS nombre_usuario,

    COUNT(*) AS total_reviews,
    ROUND(AVG(calificacion), 2) AS rating_promedio_usuario,

    -- 🧠 nivel de usuario (IMPORTANTE BI)
    CASE 
        WHEN COUNT(*) > 50 THEN 'power_user'
        WHEN COUNT(*) > 10 THEN 'activo'
        ELSE 'ocasional'
    END AS tipo_usuario,

    -- ⚠️ usuarios problemáticos
    SUM(CASE WHEN calificacion <= 2 THEN 1 ELSE 0 END) AS reviews_negativas

FROM silver.vw_clean_electronics c
JOIN silver.vw_processed_electronics p
    ON c.reviewerID = p.id_usuario
GROUP BY id_usuario;




-- consulta optimizada
CREATE OR REPLACE TABLE gold.dim_producto AS
SELECT
    asin AS id_producto,
    nombre_producto,
    marca,
    categoria_principal,
    categoria_general,

    fecha_producto,
    precio,
    caracteristicas,

    estado_fecha,
    estado_precio,
    estado_caracteristicas,
    quality_score,

    segmento_precio,
    posicion_mercado,

    -- 🧠 valor de negocio del producto
    CASE 
        WHEN precio IS NULL THEN 'sin_valor'
        WHEN precio < 10 THEN 'low_value'
        WHEN precio < 100 THEN 'medium_value'
        ELSE 'high_value'
    END AS valor_negocio,

    -- ⚠️ estado del producto
    CASE 
        WHEN quality_score >= 3 THEN 'saludable'
        WHEN quality_score = 2 THEN 'riesgo_medio'
        ELSE 'critico'
    END AS estado_producto

FROM silver.vw_enriched_meta_electronics;



-- consultas optimizadas
CREATE OR REPLACE TABLE gold.dim_tiempo AS
SELECT DISTINCT

    date_key,
    fecha_completa,
    anio,
    mes,
    dia,
    hora,
    semana,
    trimestre,

    -- 🧠 comportamiento temporal
    CASE 
        WHEN mes IN (11,12) THEN 'alta_demanda'
        WHEN mes IN (6,7,8) THEN 'media_demanda'
        ELSE 'baja_demanda'
    END AS temporada

FROM silver.vw_processed_electronics;