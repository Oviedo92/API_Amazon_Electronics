using AmazonLakehouseAPI.Security;
using AmazonLakehouseAPI.Services;
using Microsoft.AspNetCore.Mvc;

namespace AmazonLakehouseAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [ApiKey] // <--- EL CANDADO DE SEGURIDAD ESTÁ ACTIVO
    public class AnalyticsController : ControllerBase
    {
        private readonly AnalyticsService _analyticsService;

        public AnalyticsController(AnalyticsService analyticsService)
        {
            _analyticsService = analyticsService;
        }

        /*[HttpGet("master-dataset")]
        public async Task<IActionResult> GetMasterDataset()
        {
            try { return Ok(await _analyticsService.GetMasterDatasetAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }
        */
        [HttpGet("incongruencias")]
        public async Task<IActionResult> GetIncongruencias()
        {
            try { return Ok(await _analyticsService.GetIncongruenciasAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }

        [HttpGet("Sentimiento")]
        public async Task<IActionResult> GetBrandSentimentAsync()
        {
            try { return Ok(await _analyticsService.GetBrandSentimentAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }

        [HttpGet("top-products")]
        public async Task<IActionResult> GetTopProducts()
        {
            try { return Ok(await _analyticsService.GetTopProductsAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }

        [HttpGet("master-impact")]
        public async Task<IActionResult> GetMasterImpact()
        {
            try { return Ok(await _analyticsService.GetMasterImpactAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }

        [HttpGet("sentiment-evolution")]
        public async Task<IActionResult> GetSentimentEvolution()
        {
            try { return Ok(await _analyticsService.GetSentimentEvolutionAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }

        [HttpGet("user-loyalty")]
        public async Task<IActionResult> GetUserLoyalty()
        {
            try { return Ok(await _analyticsService.GetUserLoyaltyAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }

        [HttpGet("usuarios-activos")]
        public async Task<IActionResult> GetUsuariosActivos()
        {
            try { return Ok(await _analyticsService.GetUsuariosActivosAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }


        /*
        [HttpGet("evolucion")]
        public async Task<IActionResult> GetEvolucion()
        {
            try { return Ok(await _analyticsService.GetEvolucionAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }
        */
        /*
        [HttpGet("top-productos")]
        public async Task<IActionResult> GetTopProductos()
        {
            try { return Ok(await _analyticsService.GetTopProductosAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }

        [HttpGet("fidelidad")]
        public async Task<IActionResult> GetFidelidad()
        {
            try { return Ok(await _analyticsService.GetFidelidadAsync()); }
            catch (Exception ex) { return StatusCode(500, $"Error interno: {ex.Message}"); }
        }
        */
    }
}