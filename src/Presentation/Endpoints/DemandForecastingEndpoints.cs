using Hemi.Application.UseCases.DemandForecasting;
using Hemi.Infrastructure.MachineLearning;

namespace Hemi.Presentation.Endpoints;

public static class DemandForecastingEndpoints
{
    public static IEndpointRouteBuilder MapDemandForecastingEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/ml/demand").WithTags("Demand forecasting ML Lab");

        group.MapPost("/train", (TrainDemandRequest request, DemandForecastingService service) =>
        {
            try { return Results.Ok(service.Train(request)); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        group.MapPost("/predict", (PredictDemandRequest request, DemandForecastingService service) =>
        {
            try { return Results.Ok(service.Predict(request)); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
            catch (KeyNotFoundException ex) { return Results.NotFound(new { error = ex.Message }); }
        });

        return app;
    }
}
