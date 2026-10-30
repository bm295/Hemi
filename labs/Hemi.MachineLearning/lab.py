"""One-day-ahead supervised regression lab. Synthetic data; no production claims."""
import argparse
import csv
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
from sklearn.pipeline import make_pipeline
from sklearn.tree import DecisionTreeRegressor


def synthetic(seed=42):
    rng = np.random.default_rng(seed)
    rows = []
    for day in range(365):
        current = date(2025, 1, 1) + timedelta(days=day)
        for item, base in [('ribeye', 16), ('salmon', 22), ('salad', 30)]:
            quantity = max(0, round(base + (9 if current.weekday() >= 4 else 0)
                                    + rng.normal(0, 5)))
            rows.append((current, item, quantity))
    return rows


def read_sales(path):
    with path.open(newline='', encoding='utf-8-sig') as stream:
        rows = [(date.fromisoformat(r['date']), r['menu_item_id'], float(r['quantity']))
                for r in csv.DictReader(stream)]
    if not rows:
        raise ValueError('Empty dataset')
    if any(not item.strip() or not np.isfinite(q) or q < 0 for _, item, q in rows):
        raise ValueError('Item IDs must be nonempty; quantities must be finite and nonnegative')
    if len({(d, item) for d, item, _ in rows}) != len(rows):
        raise ValueError('Expected exactly one row per date/item')
    dates = sorted({d for d, _, _ in rows})
    items = {item for _, item, _ in rows}
    if (dates[-1] - dates[0]).days + 1 != len(dates) or len(rows) != len(dates) * len(items):
        raise ValueError('Expected a complete daily grid; explicitly represent verified zero-sale days')
    return sorted(rows)


def features(rows):
    history, result = {}, []
    for d, item, quantity in sorted(rows):
        previous = history.setdefault(item, [])
        if len(previous) >= 28:
            x = {'item': item, 'weekday': str(d.weekday()),
                 'lag_1': previous[-1], 'mean_7': float(np.mean(previous[-7:])),
                 'same_weekday_4': float(np.mean(previous[-28::7]))}
            result.append((d, x, quantity))
        previous.append(quantity)  # Append AFTER feature construction: no target leakage.
    return result


def split(samples):
    dates = sorted({d for d, _, _ in samples})
    if len(dates) < 60:
        raise ValueError('Need at least 60 usable dates after the 28-day warm-up')
    first, second = dates[int(len(dates) * .67)], dates[int(len(dates) * .83)]
    return ([r for r in samples if r[0] < first],
            [r for r in samples if first <= r[0] < second],
            [r for r in samples if r[0] >= second])


def xy(rows):
    return [x for _, x, _ in rows], [y for _, _, y in rows]


def experiment(rows):
    train, validation, test = split(features(rows))
    x_train, y_train = xy(train)
    x_val, y_val = xy(validation)
    candidates = {'linear': LinearRegression()}
    for depth in (2, 4, 8, None):
        candidates[f'tree_depth_{depth}'] = DecisionTreeRegressor(max_depth=depth, random_state=42)
    fitted, scores = {}, []
    for name, estimator in candidates.items():
        model = make_pipeline(DictVectorizer(sparse=False), estimator)
        model.fit(x_train, y_train)  # Preprocessing also learns from TRAIN only.
        fitted[name] = model
        scores.append({'model': name,
                       'train_mae': float(mean_absolute_error(y_train, model.predict(x_train))),
                       'validation_mae': float(mean_absolute_error(y_val, model.predict(x_val)))})
    baseline = lambda xs: [x['same_weekday_4'] for x in xs]
    scores.insert(0, {'model': 'baseline_same_weekday',
                     'train_mae': float(mean_absolute_error(y_train, baseline(x_train))),
                     'validation_mae': float(mean_absolute_error(y_val, baseline(x_val)))})
    selected = min(scores, key=lambda s: s['validation_mae'])['model']
    # Freeze selection before accessing test labels. No test scores for losing candidates.
    x_test, y_test = xy(test)
    predictions = baseline(x_test) if selected == 'baseline_same_weekday' else fitted[selected].predict(x_test)
    return {'splits': {name: {'rows': len(part), 'from': str(part[0][0]), 'to': str(part[-1][0])}
                       for name, part in zip(('train', 'validation', 'test'), (train, validation, test))},
            'candidates': scores, 'selected_by_validation': selected,
            'selected_test_mae': float(mean_absolute_error(y_test, predictions))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, help='Complete daily sales grid: date,menu_item_id,quantity')
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'output')
    args = parser.parse_args()
    rows = read_sales(args.csv) if args.csv else synthetic()
    report = experiment(rows)
    report['source'] = str(args.csv) if args.csv else 'SYNTHETIC educational dataset, seed=42'
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / 'daily-sales.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.writer(stream)
        writer.writerow(('date', 'menu_item_id', 'quantity'))
        writer.writerows(rows)
    (args.output / 'metrics.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(report['source'])
    for name, details in report['splits'].items():
        print(f'{name}: {details}')
    print(f'{"Model":26} {"Train MAE":>12} {"Validation MAE":>16}')
    for score in report['candidates']:
        print(f'{score["model"]:26} {score["train_mae"]:12.3f} {score["validation_mae"]:16.3f}')
    print(f'Selected: {report["selected_by_validation"]}; test MAE: {report["selected_test_mae"]:.3f} portions')
    print('Compare shallow vs deep trees: lower train error can accompany higher validation error.')
    print('Do not tune using the test score. Results are rolling one-day forecasts, not a multi-day forecast.')


if __name__ == '__main__':
    main()
