namespace AmazonLakehouseAPI.Models
{
    public class KpiTopProductos
    {
        public string Producto { get; set; }
        public string Marca { get; set; }
        public int Resenas_Verificadas { get; set; }
        public double Estrellas { get; set; }
        public int Votos_de_Utilidad { get; set; }
    }
}