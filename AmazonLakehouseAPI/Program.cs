using AmazonLakehouseAPI.Services;
using Microsoft.OpenApi.Models;
using Prometheus; // <-- IMPORTANTE: Necesario para el candado de Swagger

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();

// --- CONFIGURACIÓN DE SWAGGER CON API KEY ---
builder.Services.AddSwaggerGen(c =>
{
    c.AddSecurityDefinition("ApiKey", new OpenApiSecurityScheme
    {
        Description = "Pon tu API Key aquí.",
        Name = "x-api-key",
        In = ParameterLocation.Header,
        Type = SecuritySchemeType.ApiKey,
        Scheme = "ApiKeyScheme"
    });

    c.AddSecurityRequirement(new OpenApiSecurityRequirement
    {
        {
            new OpenApiSecurityScheme
            {
                Reference = new OpenApiReference
                {
                    Type = ReferenceType.SecurityScheme,
                    Id = "ApiKey"
                }
            },
            new string[] {}
        }
    });
});
// ---------------------------------------------

// ? ADIÓS REDIS: Ya no configuramos IConnectionMultiplexer aquí.

// Registrar tu Servicio de Analítica para inyección de dependencias
builder.Services.AddScoped<AnalyticsService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();

app.UseHttpMetrics();

app.MapControllers();

app.MapMetrics();

app.Run();