var builder = WebApplication.CreateBuilder(args);

// Load environment variables
builder.Configuration
    .AddJsonFile("appsettings.json")
    .AddEnvironmentVariables();

// Add services to the container.
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();
builder.Services.AddSingleton<MikrotikService.Services.MikrotikService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapControllers();

app.Run();
