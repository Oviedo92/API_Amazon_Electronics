--🟢 KPI 7 — Usuarios más activos
-- 🎯 🟢 Verde
-- 👉 power users
CREATE OR REPLACE TABLE gold.kpi_usuarios_activos AS
SELECT
    u.id_usuario,
    u.nombre_usuario,
    u.total_reviews,
    u.rating_promedio_usuario
FROM gold.dim_usuario u
ORDER BY u.total_reviews DESC
LIMIT 10;


-- 🟢 KPI 1 — Rating por marca (GOLD)
-- 🎯 Color BI: 🟢 Verde
-- 👉 mide calidad de fabricantes
CREATE OR REPLACE TABLE gold.kpi_rating_marca AS
SELECT 
    p.marca,
    COUNT(*) AS total_reviews,
    ROUND(AVG(f.rating), 2) AS rating_promedio
FROM gold.fact_reseñas f
JOIN gold.dim_producto p 
    ON f.id_producto = p.id_producto
GROUP BY p.marca;



-- 🟢 KPI 2 — Segmentación de mercado
-- 🎯 🟢 Verde
-- 👉 estrategia de precios
CREATE OR REPLACE TABLE gold.kpi_segmentacion_mercado AS
SELECT
    segmento_precio,
    COUNT(*) AS total_productos,
    ROUND(AVG(quality_score), 2) AS calidad_promedio
FROM gold.dim_producto
GROUP BY segmento_precio;


-- 🟡 KPI 3 — % Reviews negativas
-- 🎯 🟢/🔴 (alerta)
-- 👉 satisfacción del cliente
CREATE OR REPLACE TABLE gold.kpi_reviews_negativas AS
SELECT 
    ROUND(
        100.0 * SUM(es_negativo) / COUNT(*), 
    2) AS pct_negativas
FROM gold.fact_reseñas;

-- 🟢 KPI 4 — Tendencia temporal
-- 🎯 🟢 Verde
-- 👉 estacionalidad + calidad
CREATE OR REPLACE TABLE gold.kpi_evolucion_rating AS
SELECT 
    t.anio,
    t.mes,
    ROUND(AVG(f.rating), 2) AS rating_promedio,
    COUNT(*) AS volumen_reviews
FROM gold.fact_reseñas f
JOIN gold.dim_tiempo t 
    ON f.date_key = t.date_key
GROUP BY t.anio, t.mes;


-- 🟢 KPI 5 — Top productos
-- 🎯 🟢 Verde
-- 👉 productos estrella
CREATE OR REPLACE TABLE gold.kpi_top_productos AS
SELECT 
    p.nombre_producto,
    p.marca,
    COUNT(*) AS total_reviews,
    ROUND(AVG(f.rating), 2) AS rating_promedio,
    SUM(f.votos) AS votos_totales
FROM gold.fact_reseñas f
JOIN gold.dim_producto p 
    ON f.id_producto = p.id_producto
GROUP BY p.nombre_producto, p.marca
ORDER BY rating_promedio DESC, votos_totales DESC
LIMIT 20;


-- 🔴 KPI 6 — Productos problemáticos 🎯 
-- 🔴 Rojo
--👉 fallos de producto / soporte
CREATE OR REPLACE TABLE gold.kpi_productos_riesgo AS
SELECT 
    p.nombre_producto,
    p.marca,
    COUNT(*) AS total_reviews,
    ROUND(AVG(f.rating), 2) AS rating_promedio,
    SUM(CASE WHEN f.rating <= 2 THEN 1 ELSE 0 END) AS reviews_negativas
FROM gold.fact_reseñas f
JOIN gold.dim_producto p 
    ON f.id_producto = p.id_producto
GROUP BY p.nombre_producto, p.marca
HAVING AVG(f.rating) <= 3
ORDER BY reviews_negativas DESC;


-- ✅ VERSIÓN MEJORADA (PROD READY)
CREATE OR REPLACE TABLE gold.kpi_valor_interaccion AS
SELECT
    id_review,
    id_producto,
    id_usuario,

    rating,
    votos,
    compra_verificada,

    -- 🧠 ponderación real tipo marketplace
    (
        rating * 
        LOG(COALESCE(votos,1) + 1) *
        CASE WHEN compra_verificada = 1 THEN 1.2 ELSE 1 END
    ) AS valor_interaccion,

    -- 📊 nivel de impacto
    CASE 
        WHEN rating >= 4 AND votos > 50 THEN 'alto_impacto'
        WHEN rating >= 4 THEN 'medio_impacto'
        WHEN rating <= 2 THEN 'impacto_negativo'
        ELSE 'neutro'
    END AS impacto_negocio

FROM silver.vw_processed_electronics;


-- ✅ VERSIÓN PRO (RISK ENGINE)
CREATE OR REPLACE TABLE gold.kpi_riesgo_productos AS
SELECT
    id_producto,

    COUNT(*) AS total_reviews,
    AVG(rating) AS rating_promedio,
    SUM(votos) AS votos_totales,

    -- 🔴 scoring de riesgo avanzado
    CASE 
        WHEN AVG(rating) <= 2 THEN 3
        WHEN AVG(rating) <= 3 AND SUM(votos) > 100 THEN 2
        WHEN SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) > 20 THEN 3
        ELSE 0
    END AS risk_score,

    -- 🚨 categoría de riesgo
    CASE 
        WHEN AVG(rating) <= 2 THEN 'critico'
        WHEN AVG(rating) <= 3 THEN 'alto_riesgo'
        WHEN SUM(votos) > 200 AND AVG(rating) < 3.5 THEN 'riesgo_volumen'
        ELSE 'estable'
    END AS estado_riesgo

FROM silver.vw_processed_electronics
GROUP BY id_producto;


-- KPI EXTRA(ESTEROIDES + BONUS)
-- 📦 KPI — PRODUCT HEALTH SCORE (tipo Amazon internal metric)
CREATE OR REPLACE TABLE gold.kpi_product_health AS
SELECT
    p.id_producto,
    p.nombre_producto,
    p.marca,

    AVG(f.rating) AS rating_promedio,
    COUNT(*) AS total_reviews,

    -- 🧠 health score global
    (
        AVG(f.rating) * 0.6 +
        LOG(COUNT(*) + 1) * 0.2 +
        (1 - COALESCE(r.risk_score,0)/3.0) * 0.2
    ) AS health_score,

    -- 🟢 estado del producto
    CASE 
        WHEN AVG(f.rating) >= 4 THEN 'excelente'
        WHEN AVG(f.rating) >= 3 THEN 'bueno'
        WHEN AVG(f.rating) >= 2 THEN 'malo'
        ELSE 'critico'
    END AS estado_producto

FROM silver.vw_processed_electronics f
JOIN gold.dim_producto p
    ON f.id_producto = p.id_producto
LEFT JOIN gold.kpi_riesgo_productos r
    ON f.id_producto = r.id_producto

GROUP BY p.id_producto, p.nombre_producto, p.marca, r.risk_score;


-- 💰 KPI 1: Valor total generado
SELECT SUM(valor_interaccion) AS valor_total
FROM gold.fact_reseñas;


-- ⭐ KPI 2: Rating por marca
SELECT 
    p.marca,
    ROUND(AVG(f.rating), 2) AS rating_promedio
FROM gold.fact_reseñas f
JOIN gold.dim_producto p ON f.id_producto = p.id_producto
GROUP BY p.marca;


-- ⚠️ KPI 3: Productos en riesgo
SELECT 
    COUNT(*) AS productos_problematicos
FROM gold.fact_reseñas
WHERE flag_riesgo = 1;


-- 📦 KPI 4: Segmentación de mercado
SELECT 
    segmento_precio,
    COUNT(*) AS total_productos,
    ROUND(AVG(quality_score),2) AS calidad_promedio
FROM gold.dim_producto
GROUP BY segmento_precio;



-- 📉 KPI 5: % reviews negativas
SELECT 
    ROUND(100.0 * SUM(es_negativo) / COUNT(*), 2) AS pct_negativas
FROM gold.fact_reseñas;


-- 🧠 KPI 6: Usuarios más valiosos
SELECT 
    id_usuario,
    total_reviews,
    rating_promedio_usuario
FROM gold.dim_usuario
ORDER BY total_reviews DESC
LIMIT 10;



-- 📊 KPI 7: Tendencia temporal
SELECT 
    t.anio,
    t.mes,
    ROUND(AVG(f.rating),2) AS rating_promedio
FROM gold.fact_reseñas f
JOIN gold.dim_tiempo t ON f.date_key = t.date_key
GROUP BY t.anio, t.mes;


-- 🏆 TOP productos
SELECT 
    p.nombre_producto,
    ROUND(AVG(f.rating),2) AS rating_promedio
FROM gold.fact_reseñas f
JOIN gold.dim_producto p ON f.id_producto = p.id_producto
GROUP BY p.nombre_producto
ORDER BY rating_promedio DESC
LIMIT 10;



--🔴 TOP productos problemáticos
SELECT 
    p.nombre_producto,
    COUNT(*) AS reviews_negativas
FROM gold.fact_reseñas f
JOIN gold.dim_producto p ON f.id_producto = p.id_producto
WHERE f.flag_riesgo = 1
GROUP BY p.nombre_producto
ORDER BY reviews_negativas DESC
LIMIT 10;





-- 
