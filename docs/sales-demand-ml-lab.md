# Sales Demand Prediction — supervised learning lab

This module is a learning exercise inside the existing Hemi solution, not a production purchasing forecast.

## Objective

Predict a menu item's **daily quantity sold** (regression label) from its calendar features (day of week, month, days since training origin) and unit price. Historical daily quantities are labeled examples; quantity is **not** included among prediction features. Each menu item is trained independently.

## Data contract

`POST /ml/demand/train` accepts:

```json
{
  "menuItemId": "11111111-1111-1111-1111-111111111111",
  "observations": [
    { "date": "2026-01-01", "menuItemId": "11111111-1111-1111-1111-111111111111", "unitPrice": 120000, "quantity": 14 }
  ]
}
```

Supply **at least 30 distinct dates** (example above shows the shape, not enough training data). Each row is one menu item/day, including zero-sales days. Aggregate duplicate transactions into daily rows *before* submitting; do not mix items or send refunds as negative sales. Prefer settled, completed orders only. The module intentionally accepts an explicit dataset rather than silently claiming that demo restaurant orders constitute sufficient historical training data.

## Train / validation / test

Data is sorted chronologically, regardless of upload order:

- First 70%: **train**, used to fit both SDCA linear and FastTree regression models.
- Next 15%: **validation**, used to compare MAE and choose the candidate.
- Last ~15%: **test**, held out from training and model selection and evaluated once on the winner.

For a production deployment, also avoid features whose values would not be known at forecast time, group records by service date when training across items, and use rolling-origin evaluation for seasonal changes.

The response reports training/validation MAE, RMSE and R² for each candidate, the selected candidate, held-out test metrics, and a **heuristic** overfitting warning when validation MAE is more than 1.5× training MAE and the gap exceeds 0.5 units. A warning is not a proof; inspect temporal drift and sample size. Comparing models on test results would invalidate the holdout.

## Predict

After training in the same running API process:

```json
POST /ml/demand/predict
{
  "menuItemId": "11111111-1111-1111-1111-111111111111",
  "date": "2026-04-15",
  "unitPrice": 120000
}
```

Predicted quantities are clamped at zero. Models live **in memory** and are lost on restart. The demo does not automatically retrain, persist artifacts, authenticate endpoints, import sales history, or support production inventory decisions. Protect training endpoints and persist/version approved models before production use.

## Run

```bash
dotnet test Hemi.sln
dotnet run --project src/Presentation/Hemi.Presentation.csproj
```

Open the Angular dashboard and select **ML Lab**. The UI generates a clearly labeled *synthetic* daily sales dataset, trains both models and displays metrics; use the predict form to forecast a day. This data is not Hemi's actual sales.
