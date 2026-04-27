using AmazonLakehouseAPI.Models;
using Dapper;
using DuckDB.NET.Data;
using System.Diagnostics; // <-- IMPORTANTE: Lo agregamos para medir el tiempo exacto
using System.Linq; // <-- IMPORTANTE: Para contar los resultados

namespace AmazonLakehouseAPI.Services
{
    public class AnalyticsService
    {
        private readonly string _duckDbConnectionString;
        private readonly ILogger<AnalyticsService> _logger;

        // ⚡ ADIÓS REDIS: Constructor limpio, solo inyectamos la configuración y el logger
        public AnalyticsService(IConfiguration configuration, ILogger<AnalyticsService> logger)
        {
            _duckDbConnectionString = configuration.GetConnectionString("DuckDBConnection");
            _logger = logger;
        }


        // ==========================================
        // MOTOR GENÉRICO DE CONSULTAS (100% DUCKDB)
        // ==========================================
        private async Task<IEnumerable<T>> EjecutarConsultaAsync<T>(string queryName, string sql)
        {
            // Dejamos espacios en blanco para que respire el texto en la consola
            _logger.LogInformation("");
            _logger.LogInformation("=======================================================");
            _logger.LogInformation($"[>] PASO 1 | SOLICITUD RECIBIDA: Data Mart '{queryName.ToUpper()}'");
            _logger.LogInformation($"[*] PASO 2 | CONECTANDO A DUCKDB Y EJECUTANDO PARQUET...");

            var stopwatch = Stopwatch.StartNew();

            using var connection = new DuckDBConnection(_duckDbConnectionString);
            connection.Open();

            var result = (await connection.QueryAsync<T>(sql)).ToList();

            stopwatch.Stop();

            _logger.LogInformation($"[+] PASO 3 | PROCESAMIENTO EXITOSO. Filas extraídas: {result.Count}");
            _logger.LogInformation($"[~] PASO 4 | RENDIMIENTO OLAP: Completado en {stopwatch.ElapsedMilliseconds} ms");
            _logger.LogInformation("=======================================================");
            _logger.LogInformation("");

            return result;
        }

        // ==========================================
        // LAS 5 CONSULTAS DEL NEGOCIO
        // ==========================================


        // Incongruencias de Marca (¡LA ESTRELLA DEL SHOW!)
        public Task<IEnumerable<KpiIncongruencias>> GetIncongruenciasAsync() =>
            EjecutarConsultaAsync<KpiIncongruencias>("Incongruencias",
                @"SELECT 
                 Marca, 
                 Alerta_Incongruencia AS Tipo_Alerta, 
                 Total_Resenas, 
                 Promedio_Estrellas, 
                 Promedio_Polaridad 
                 FROM gold.kpi_brand_incongruence"
     );

        // 1. Sentimiento de Marca
        public Task<IEnumerable<KpiBrandSentiment>> GetBrandSentimentAsync() =>
            EjecutarConsultaAsync<KpiBrandSentiment>("SentimientoMarca", "SELECT * FROM gold.kpi_brand_sentiment");

        // 2. Evolución Mensual
        public Task<IEnumerable<KpiSentimentEvolution>> GetSentimentEvolutionAsync() =>
            EjecutarConsultaAsync<KpiSentimentEvolution>("EvolucionMensual", "SELECT * FROM gold.kpi_sentiment_evolution");

        // 3. Top Productos Populares
        public Task<IEnumerable<KpiTopProductos>> GetTopProductsAsync() =>
            EjecutarConsultaAsync<KpiTopProductos>("TopProductos", "SELECT * FROM gold.kpi_top_products");

        // 4. Fidelidad de Usuario
        public Task<IEnumerable<KpiFidelidadUsuario>> GetUserLoyaltyAsync() =>
            EjecutarConsultaAsync<KpiFidelidadUsuario>("FidelidadUsuario", "SELECT * FROM gold.kpi_user_loyalty");

        // 5. Impacto / Master Dataset
        public Task<IEnumerable<KpiMasterDataset>> GetMasterImpactAsync() =>
            EjecutarConsultaAsync<KpiMasterDataset>("MasterImpact", "SELECT * FROM gold.kpi_master_impact");


        public Task<IEnumerable<KpiUsuariosActivos>> GetUsuariosActivosAsync() =>
    EjecutarConsultaAsync<KpiUsuariosActivos>(
        "UsuariosActivos",
        @"SELECT 
            id_usuario AS IdUsuario,
            nombre_usuario AS NombreUsuario,
            total_reviews AS TotalReviews,
            rating_promedio_usuario AS RatingPromedioUsuario
          FROM goldd.kpi_usuarios_activos"
    );

        /*
        public Task<IEnumerable<KpiMasterDataset>> GetMasterDatasetAsync() =>
            EjecutarConsultaAsync<KpiMasterDataset>("Master_Dataset",
                "SELECT t.Anio, p.brand AS Marca, s.Categoria_Texto AS Sentimiento, COUNT(f.IdResena) AS Total_Interacciones, SUM(f.vote) AS Impacto_Votos FROM Fact_Resenas f JOIN Dim_Producto p ON f.asin = p.asin JOIN Dim_Tiempo t ON f.DateKey = t.DateKey JOIN Dim_Sentimiento s ON f.SentimentKey = s.SentimentKey WHERE p.brand IS NOT NULL GROUP BY t.Anio, p.brand, s.Categoria_Texto ORDER BY Total_Interacciones DESC LIMIT 50000;");
        
        public Task<IEnumerable<KpiEvolucionSentimiento>> GetEvolucionAsync() =>
            EjecutarConsultaAsync<KpiEvolucionSentimiento>("Evolucion_Sentimiento",
                "SELECT t.Anio, t.Mes, p.category AS Categoria, COUNT(f.IdResena) AS Volumen_Resenas, ROUND(AVG(f.Score_Polaridad), 4) AS Promedio_Polaridad_Mensual FROM Fact_Resenas f JOIN Dim_Tiempo t ON f.DateKey = t.DateKey JOIN Dim_Producto p ON f.asin = p.asin GROUP BY t.Anio, t.Mes, p.category ORDER BY t.Anio DESC, t.Mes DESC;");

        public Task<IEnumerable<KpiTopProductos>> GetTopProductosAsync() =>
            EjecutarConsultaAsync<KpiTopProductos>("Top_Productos",
                "SELECT p.title AS Producto, p.brand AS Marca, COUNT(f.IdResena) AS Resenas_Verificadas, ROUND(AVG(f.overall), 2) AS Estrellas, SUM(f.vote) AS Votos_de_Utilidad FROM Fact_Resenas f JOIN Dim_Producto p ON f.asin = p.asin WHERE f.verified = true AND p.title IS NOT NULL GROUP BY p.title, p.brand HAVING COUNT(f.IdResena) > 100 ORDER BY Estrellas DESC, Resenas_Verificadas DESC LIMIT 10;");

        public Task<IEnumerable<KpiFidelidadUsuario>> GetFidelidadAsync() =>
            EjecutarConsultaAsync<KpiFidelidadUsuario>("Fidelidad_Usuario",
                "SELECT u.reviewerID AS ID_Usuario, COUNT(f.IdResena) AS Cantidad_Resenas_Escritas, s.Categoria_Texto AS Sentimiento_Predominante FROM Fact_Resenas f JOIN Dim_Usuario u ON f.reviewerID = u.reviewerID JOIN Dim_Sentimiento s ON f.SentimentKey = s.SentimentKey GROUP BY u.reviewerID, s.Categoria_Texto ORDER BY Cantidad_Resenas_Escritas DESC LIMIT 15;");
        */
    }
}