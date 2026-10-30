import unittest
from datetime import timedelta

from lab import experiment, features, split, synthetic


class LearningLabTests(unittest.TestCase):
    def test_target_changes_do_not_change_current_or_past_features(self):
        rows = synthetic()
        cutoff = rows[300][0]
        changed = [(d, i, q + 1000 if d >= cutoff else q) for d, i, q in rows]
        before = [(d, x) for d, x, _ in features(rows) if d <= cutoff]
        after = [(d, x) for d, x, _ in features(changed) if d <= cutoff]
        self.assertEqual(before, after)

    def test_splits_keep_dates_disjoint_and_ordered(self):
        train, validation, test = split(features(synthetic()))
        self.assertLess(train[-1][0], validation[0][0])
        self.assertLess(validation[-1][0], test[0][0])
        self.assertEqual(len(train) + len(validation) + len(test), 337 * 3)

    def test_weekday_baseline_uses_previous_four_same_weekdays(self):
        rows = synthetic()
        d, x, _ = features(rows)[0]
        expected = [q for day, item, q in rows
                    if item == x['item'] and day in [d - timedelta(days=n) for n in (7, 14, 21, 28)]]
        self.assertEqual(x['same_weekday_4'], sum(expected) / 4)

    def test_held_out_test_labels_do_not_change_model_selection(self):
        rows = synthetic()
        boundary = split(features(rows))[2][0][0]
        changed = [(d, i, q + 1000 if d >= boundary else q) for d, i, q in rows]
        a, b = experiment(rows), experiment(changed)
        self.assertEqual(a['candidates'], b['candidates'])
        self.assertEqual(a['selected_by_validation'], b['selected_by_validation'])
        self.assertNotEqual(a['selected_test_mae'], b['selected_test_mae'])


if __name__ == '__main__':
    unittest.main()
