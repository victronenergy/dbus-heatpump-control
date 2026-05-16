import os
import sys
import unittest
from unittest.mock import patch

# aiovelib must be available before utils import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ext', 'aiovelib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils import HeatpumpPowerEstimator, Power

class TestHeatpumpPowerEstimator(unittest.TestCase):
    def test_recreate_keeps_expected_total(self):
        est = HeatpumpPowerEstimator(
            nominal_total_w=2000,
            phases=1,
            expected_floor_w=0,
        )
        est._expected_total = 1234.0

        new_est = est.recreate(phases=3, keep_expected=True)

        self.assertAlmostEqual(new_est.expected_P_total, 1234.0)
        self.assertEqual(new_est.phases, 3)

    def test_significant_change_detects_downward_shift(self):
        est = HeatpumpPowerEstimator(
            nominal_total_w=2000,
            phases=1,
            expected_floor_w=0,
            significant_abs_w=1000,
            significant_rel=0.10,
        )
        est._last_reported_expected_total = 1000.0

        self.assertTrue(est._significant_change(850.0))

    def test_off_mode_mean_learns_across_full_off_range(self):
        est = HeatpumpPowerEstimator(
            nominal_total_w=600,
            phases=None,
            expected_floor_w=0,
            alpha=1.0,
            target_mode="mean",
            min_samples=3,
            learn_when_running=False,
        )

        # First sample only initializes timing baseline.
        est.feed(power=Power(0, None, None))

        # OFF-mode learns all samples, independent of running threshold.
        est.feed(power=Power(0, None, None))
        est.feed(power=Power(300, None, None))
        est.feed(power=Power(0, None, None))

        self.assertAlmostEqual(est.expected_P_total, 100.0)

        # > threshold sample is also included for OFF-mode learning.
        est.feed(power=Power(1500, None, None))
        self.assertAlmostEqual(est.expected_P_total, 450.0)

    def test_on_mode_mean_also_learns_independent_of_threshold(self):
        est = HeatpumpPowerEstimator(
            nominal_total_w=2000,
            phases=None,
            expected_floor_w=0,
            alpha=1.0,
            target_mode="mean",
            min_samples=3,
            learn_when_running=True,
        )

        # First sample initializes timing baseline.
        est.feed(power=Power(0, None, None))

        # ON-mode learns all samples by default (relay-state-conditioned).
        est.feed(power=Power(100, None, None))
        est.feed(power=Power(1200, None, None))
        est.feed(power=Power(200, None, None))

        self.assertAlmostEqual(est.expected_P_total, 500.0)

    @patch("utils.time.monotonic")
    def test_settling_time_blocks_learning_after_relay_change(self, mock_monotonic):
        est = HeatpumpPowerEstimator(
            nominal_total_w=2000,
            phases=1,
            expected_floor_w=0,
            alpha=1.0,
            target_mode="mean",
            min_samples=1,
            settling_time_s=300,
        )

        # Baseline sample initializes timing.
        mock_monotonic.side_effect = [0.0, 1.0, 301.0]
        self.assertFalse(est.feed(power=Power(1000, None, None)))

        est.mark_relay_state_change()
        prev = est.expected_P_total

        # During settling window: no learning.
        self.assertFalse(est.feed(power=Power(1500, None, None)))
        self.assertAlmostEqual(est.expected_P_total, prev)

        # After settling window: learning resumes.
        est.feed(power=Power(1500, None, None))
        self.assertAlmostEqual(est.expected_P_total, 1500.0)

    @patch("utils.time.monotonic")
    def test_zero_settling_time_disables_blocking(self, mock_monotonic):
        est = HeatpumpPowerEstimator(
            nominal_total_w=2000,
            phases=1,
            expected_floor_w=0,
            alpha=1.0,
            target_mode="mean",
            min_samples=1,
            settling_time_s=0,
        )

        mock_monotonic.side_effect = [0.0, 1.0]
        self.assertFalse(est.feed(power=Power(1000, None, None)))

        est.mark_relay_state_change()
        est.feed(power=Power(1600, None, None))
        self.assertAlmostEqual(est.expected_P_total, 1600.0)


if __name__ == "__main__":
    unittest.main()
