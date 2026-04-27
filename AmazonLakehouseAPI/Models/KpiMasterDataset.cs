namespace AmazonLakehouseAPI.Models
{
    public class KpiMasterDataset
    {
        public int Anio { get; set; }
        public string Marca { get; set; }
        public string Sentimiento { get; set; }
        public int Total_Interacciones { get; set; }
        public int Impacto_Votos { get; set; }
    }
}