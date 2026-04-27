namespace AmazonLakehouseAPI.Models
{
    public class KpiIncongruencias
    {
        public string Marca { get; set; }
        public string Tipo_Alerta { get; set; }
        public int Total_Resenas { get; set; }
        public double Promedio_Estrellas { get; set; }
        public double Promedio_Polaridad { get; set; }
    }
}