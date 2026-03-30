var builder = WebApplication.CreateBuilder(args);

// Load environment variables
builder.Configuration
    .AddJsonFile("appsettings.json")
    .AddEnvironmentVariables();

// Configure Logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();
builder.Logging.SetMinimumLevel(LogLevel.Information);

// Add services to the container.
builder.Services.AddEndpointsApiExplorer(); // Added for better Swagger support
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();
builder.Services.AddSingleton<MikrotikService.Services.MikrotikService>();

var app = builder.Build();

// ENABLE SWAGGER FOR ALL ENVIRONMENTS (Production & Development)
app.UseSwagger();
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "Mikrotik API V1");
    c.RoutePrefix = string.Empty; // This makes Swagger the home page (e.g., http://your-aws-ip:5000)
});

// REMOVED: if (app.Environment.IsDevelopment()) check

app.UseHttpsRedirection();
app.MapControllers();
app.Run();
