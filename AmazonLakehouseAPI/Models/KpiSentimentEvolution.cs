namespace AmazonLakehouseAPI.Models
{
    public class KpiSentimentEvolution
    {
        public int Anio { get; set; }
        public int Mes { get; set; }
        public string Categoria { get; set; }
        public long Volumen_Resenas { get; set; }
        public double Promedio_Polaridad_Mensual { get; set; }
    }
}
