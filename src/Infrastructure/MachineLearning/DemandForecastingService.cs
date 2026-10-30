using Hemi.Application.UseCases.DemandForecasting;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace Hemi.Infrastructure.MachineLearning;

// An educational, process-local model registry. Restarting the API clears trained models.
public sealed class DemandForecastingService
{
    private readonly MLContext _ml = new(seed: 42);
    private readonly object _gate = new();
    private readonly Dictionary<Guid, TrainedDemandModel> _models = new();

    public DemandTrainingResult Train(TrainDemandRequest request)
    {
        if (request.MenuItemId == Guid.Empty || request.Observations is null)
            throw new ArgumentException("A menu item and observations are required.");

        var observations = request.Observations.ToArray();
        if (observations.Any(x => x.MenuItemId != request.MenuItemId ||
            !float.IsFinite(x.Quantity) || x.Quantity < 0 ||
            !float.IsFinite(x.UnitPrice) || x.UnitPrice <= 0))
            throw new ArgumentException("All observations must match the menu item and have valid non-negative quantities and positive prices.");

        var (train, validation, test) = ChronologicalDemandSplit.Split(observations, x => x.Date);
        var origin = train[0].Date;
        DemandRow[] Convert(DailyDemandObservation[] items) =>
            items.Select(x => ToRow(x.Date, x.UnitPrice, x.Quantity, origin)).ToArray();

        var trainData = _ml.Data.LoadFromEnumerable(Convert(train));
        var validationData = _ml.Data.LoadFromEnumerable(Convert(validation));
        var testData = _ml.Data.LoadFromEnumerable(Convert(test));

        var features = _ml.Transforms.Concatenate("Features",
            nameof(DemandRow.DayOfWeek), nameof(DemandRow.Month),
            nameof(DemandRow.DayIndex), nameof(DemandRow.UnitPrice))
            .Append(_ml.Transforms.NormalizeMinMax("Features"));

        var candidates = new (string Name, IEstimator<ITransformer> Pipeline)[]
        {
            ("SDCA linear regression", features.Append(_ml.Regression.Trainers.Sdca(
                labelColumnName: nameof(DemandRow.Label), featureColumnName: "Features"))),
            ("FastTree regression", features.Append(_ml.Regression.Trainers.FastTree(
                labelColumnName: nameof(DemandRow.Label), featureColumnName: "Features",
                numberOfLeaves: 8, numberOfTrees: 50, minimumExampleCountPerLeaf: 3)))
        };

        var results = candidates.Select(candidate =>
        {
            var model = candidate.Pipeline.Fit(trainData);
            var trainingMetrics = Evaluate(model, trainData);
            var validationMetrics = Evaluate(model, validationData);
            return (candidate.Name, Model: model, Training: trainingMetrics, Validation: validationMetrics);
        }).ToArray();

        // Model selection uses validation only. Test data is evaluated once, after selection.
        var winner = results.OrderBy(x => x.Validation.Mae).First();
        var testMetrics = Evaluate(winner.Model, testData);
        var summary = new DemandTrainingResult(
            request.MenuItemId, train.Length, validation.Length, test.Length,
            train[^1].Date, validation[^1].Date, winner.Name,
            results.Select(x => new DemandCandidateResult(x.Name, x.Training, x.Validation)).ToArray(),
            testMetrics,
            winner.Validation.Mae > winner.Training.Mae * 1.5 &&
                winner.Validation.Mae - winner.Training.Mae > 0.5);

        lock (_gate)
            _models[request.MenuItemId] = new TrainedDemandModel(winner.Name, winner.Model, origin);

        return summary;
    }

    public DemandPrediction Predict(PredictDemandRequest request)
    {
        if (request.MenuItemId == Guid.Empty || !float.IsFinite(request.UnitPrice) || request.UnitPrice <= 0)
            throw new ArgumentException("A valid menu item and positive unit price are required.");

        TrainedDemandModel trained;
        lock (_gate)
        {
            if (!_models.TryGetValue(request.MenuItemId, out var found))
                throw new KeyNotFoundException("Train a model for this menu item before predicting.");
            trained = found;
        }

        // PredictionEngine is not thread-safe: create one per request.
        var engine = _ml.Model.CreatePredictionEngine<DemandRow, DemandScore>(trained.Model);
        var score = engine.Predict(ToRow(request.Date, request.UnitPrice, 0, trained.Origin)).Score;
        return new DemandPrediction(request.MenuItemId, request.Date, Math.Max(0, score), trained.Name);
    }

    private DemandMetrics Evaluate(ITransformer model, IDataView data)
    {
        var metrics = _ml.Regression.Evaluate(model.Transform(data), labelColumnName: nameof(DemandRow.Label));
        return new DemandMetrics(metrics.MeanAbsoluteError, metrics.RootMeanSquaredError, metrics.RSquared);
    }

    private static DemandRow ToRow(DateOnly date, float price, float quantity, DateOnly origin) => new()
    {
        DayOfWeek = (float)date.DayOfWeek,
        Month = date.Month,
        DayIndex = date.DayNumber - origin.DayNumber,
        UnitPrice = price,
        Label = quantity
    };

    private sealed record TrainedDemandModel(string Name, ITransformer Model, DateOnly Origin);

    private sealed class DemandRow
    {
        public float DayOfWeek { get; set; }
        public float Month { get; set; }
        public float DayIndex { get; set; }
        public float UnitPrice { get; set; }
        public float Label { get; set; }
    }

    private sealed class DemandScore
    {
        [ColumnName("Score")]
        public float Score { get; set; }
    }
}
