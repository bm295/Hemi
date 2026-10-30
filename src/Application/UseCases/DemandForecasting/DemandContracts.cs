namespace Hemi.Application.UseCases.DemandForecasting;

// One row represents one menu item's sales for one calendar day.
// A zero-quantity row is meaningful; do not silently omit days with no sales.
public sealed record DailyDemandObservation(DateOnly Date, Guid MenuItemId, float UnitPrice, float Quantity);

public sealed record TrainDemandRequest(Guid MenuItemId, IReadOnlyList<DailyDemandObservation> Observations);

public sealed record PredictDemandRequest(Guid MenuItemId, DateOnly Date, float UnitPrice);

public sealed record DemandMetrics(double Mae, double Rmse, double RSquared);

public sealed record DemandCandidateResult(string Name, DemandMetrics Training, DemandMetrics Validation);

public sealed record DemandTrainingResult(
    Guid MenuItemId,
    int TrainingCount,
    int ValidationCount,
    int TestCount,
    DateOnly TrainingThrough,
    DateOnly ValidationThrough,
    string SelectedModel,
    IReadOnlyList<DemandCandidateResult> Candidates,
    DemandMetrics Test,
    bool PossibleOverfitting);

public sealed record DemandPrediction(Guid MenuItemId, DateOnly Date, float PredictedQuantity, string ModelName);

public static class ChronologicalDemandSplit
{
    public static (T[] Training, T[] Validation, T[] Test) Split<T>(
        IEnumerable<T> observations, Func<T, DateOnly> dateSelector)
    {
        var ordered = observations.OrderBy(dateSelector).ToArray();
        if (ordered.Length < 30)
            throw new ArgumentException("At least 30 daily observations are required.");

        for (var i = 1; i < ordered.Length; i++)
            if (dateSelector(ordered[i]) == dateSelector(ordered[i - 1]))
                throw new ArgumentException("Only one observation per menu item and date is allowed.");

        var trainingCount = (int)Math.Floor(ordered.Length * 0.70);
        var validationCount = (int)Math.Floor(ordered.Length * 0.15);
        return (
            ordered[..trainingCount],
            ordered[trainingCount..(trainingCount + validationCount)],
            ordered[(trainingCount + validationCount)..]);
    }
}
