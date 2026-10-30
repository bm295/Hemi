using Hemi.Application.UseCases.DemandForecasting;
using Hemi.Infrastructure.MachineLearning;

namespace Hemi.Tests;

public sealed class DemandForecastingTests
{
    [Fact]
    public void Split_IsChronologicalAndHasNoOverlappingDates()
    {
        var input = Enumerable.Range(0, 40)
            .Select(i => new DailyDemandObservation(new DateOnly(2026, 1, 1).AddDays(39 - i), Guid.Empty, 100, i))
            .ToArray();

        var (train, validation, test) = ChronologicalDemandSplit.Split(input, x => x.Date);

        Assert.Equal(28, train.Length);
        Assert.Equal(6, validation.Length);
        Assert.Equal(6, test.Length);
        Assert.True(train[^1].Date < validation[0].Date);
        Assert.True(validation[^1].Date < test[0].Date);
    }

    [Fact]
    public void Split_RejectsDuplicateDates()
    {
        var input = Enumerable.Range(0, 30).Select(i =>
            new DailyDemandObservation(new DateOnly(2026, 1, 1).AddDays(i / 2), Guid.Empty, 100, 5));
        Assert.Throws<ArgumentException>(() => ChronologicalDemandSplit.Split(input, x => x.Date));
    }

    [Fact]
    public void Training_ProducesMetricsAndPrediction()
    {
        var id = Guid.NewGuid();
        var rows = Enumerable.Range(0, 100).Select(i =>
        {
            var date = new DateOnly(2026, 1, 1).AddDays(i);
            var quantity = 12 + (date.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday ? 9 : 0) + i / 20;
            return new DailyDemandObservation(date, id, 120_000, quantity);
        }).ToArray();

        var service = new DemandForecastingService();
        var result = service.Train(new TrainDemandRequest(id, rows));
        var prediction = service.Predict(new PredictDemandRequest(id, new DateOnly(2026, 4, 12), 120_000));

        Assert.Equal(70, result.TrainingCount);
        Assert.Equal(15, result.ValidationCount);
        Assert.Equal(15, result.TestCount);
        Assert.Equal(2, result.Candidates.Count);
        Assert.True(double.IsFinite(result.Test.Mae));
        Assert.True(prediction.PredictedQuantity >= 0);
    }
}
