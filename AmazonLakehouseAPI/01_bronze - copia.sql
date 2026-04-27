-- ============================================================
-- CAPA BRONZE - METODOLOGÍA MEDALLION
-- ============================================================
-- Objetivo:
-- Ingerir datos en su estado más crudo posible (raw data),
-- sin transformaciones complejas, manteniendo fidelidad
-- a la fuente original.
--
-- En esta capa:
-- ✔ Se permite datos sucios/inconsistentes
-- ✔ No se aplican reglas de negocio
-- ✔ Se prioriza trazabilidad y volumen
-- ✔ Se trabaja con esquemas flexibles (JSON, logs, etc.)
--
-- Caso: Dataset de Amazon Electronics
-- - electronics: reseñas (reviews)
-- - meta_electronics: metadatos de productos
-- ============================================================


-- ------------------------------------------------------------
-- 1. Creación del esquema BRONZE
-- ------------------------------------------------------------
-- Se define un espacio lógico para agrupar los datos crudos
-- siguiendo la arquitectura medallion (bronze/silver/gold)
CREATE SCHEMA bronze;


-- ------------------------------------------------------------
-- 2. Ingesta de reseñas (electronics)
-- ------------------------------------------------------------
-- Se crea una VIEW (no tabla) para:
-- ✔ Evitar duplicación de datos
-- ✔ Leer directamente desde el archivo JSON
-- ✔ Mantener datos siempre actualizados desde la fuente
--
-- read_json_auto:
-- Detecta automáticamente el esquema del JSON
-- (útil para datos semi-estructurados)
CREATE OR REPLACE VIEW bronze.electronics AS
SELECT *
FROM read_json_auto('C:\Users\wORK\Documents\repositorio_python\etl_env\Electronics.json');


-- ------------------------------------------------------------
-- 3. Ingesta de metadatos (meta_electronics)
-- ------------------------------------------------------------
-- Contiene información descriptiva de productos:
-- título, categoría, marca, precio, etc.
--
-- Se mantiene sin transformación para respetar el principio
-- de "raw ingestion"
CREATE OR REPLACE VIEW bronze.meta_electronics AS
SELECT *
FROM read_json_auto('C:\Users\wORK\Documents\repositorio_python\etl_env\meta_Electronics.json');


-- ------------------------------------------------------------
-- 4. Validación de volumen de datos
-- ------------------------------------------------------------
-- Se verifica cantidad de registros ingeridos
-- Esto ayuda a:
-- ✔ Validar integridad de carga
-- ✔ Detectar errores de lectura
-- ✔ Tener línea base para monitoreo
SELECT COUNT(*) FROM bronze.electronics;
-- ✔ Esperado: ~20.9 millones de registros (reseñas)

SELECT COUNT(*) FROM bronze.meta_electronics;
-- ✔ Esperado: ~786 mil registros (metadatos)


-- ------------------------------------------------------------
-- 5. Exploración de estructura (schema discovery)
-- ------------------------------------------------------------
-- Se inspecciona la estructura inferida por DuckDB:
-- ✔ Tipos de datos
-- ✔ Campos disponibles
-- ✔ Posibles inconsistencias
--
-- Paso clave antes de pasar a SILVER
DESCRIBE bronze.electronics;

DESCRIBE bronze.meta_electronics;


-- ------------------------------------------------------------
-- 6. Muestreo de datos
-- ------------------------------------------------------------
-- Se revisan registros reales para:
-- ✔ Entender calidad de datos
-- ✔ Detectar nulos, formatos incorrectos
-- ✔ Identificar campos relevantes para negocio
--
-- Importante: LIMIT para evitar sobrecarga
SELECT * FROM bronze.electronics LIMIT 10000;

SELECT * FROM bronze.meta_electronics LIMIT 10000;


-- ------------------------------------------------------------
-- 7. Inspección técnica del esquema (PRAGMA)
-- ------------------------------------------------------------
-- PRAGMA table_info permite obtener metadata detallada
-- de la estructura de la tabla/vista:
-- ✔ Tipos de datos inferidos automáticamente
-- ✔ Restricciones (NULL / NOT NULL)
-- ✔ Posición de columnas
--
-- Es especialmente útil en datos JSON, donde el esquema
-- es inferido y puede contener inconsistencias.
--
-- Este paso ayuda a:
-- ✔ Detectar errores de tipado (ej: números como texto)
-- ✔ Preparar transformaciones en capa SILVER
-- ✔ Validar calidad estructural del dataset
PRAGMA table_info('bronze.electronics');

PRAGMA table_info('bronze.meta_electronics');