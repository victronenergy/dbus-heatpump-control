from __future__ import annotations

import time
from dataclasses import dataclass
from collections import deque
from typing import Deque, Optional, Tuple, Iterable


@dataclass(frozen=True, slots=True)
class Power:
    l1: int | None
    l2: int | None
    l3: int | None

    @property
    def total(self) -> int:
        return int(sum(p for p in (self.l1, self.l2, self.l3) if p is not None))

    @property
    def valid(self) -> bool:
        return any((self.l1, self.l2, self.l3))


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _quantile(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    # simple "nearest rank" quantile
    idx = max(0, min(len(s) - 1, int((len(s) - 1) * q)))
    return float(s[idx])


class HeatpumpPowerEstimator:
    """
    Expected running power estimator.

      - Consider HP "running" when P_total > running_threshold_w
      - Keep a rolling window of running samples (time-based)
      - target = quantile(window, q)
      - expected_P_total = EWMA toward target
      - per-phase estimate derived from expected total
    """

    def __init__(
        self,
        nominal_total_w: float,
        phases: Optional[int] = None,
        *,
        running_threshold_w: Optional[float] = None,
        window_s: float = 600.0,
        quantile_q: float = 0.75,
        alpha: float = 0.05,
        expected_floor_w: float = 300.0,
        expected_cap_mult: float = 1.5,

        # "significant change" thresholds for feed() return value
        significant_abs_w: float = 150.0,
        significant_rel: float = 0.10,
    ):
        if nominal_total_w <= 0:
            raise ValueError("nominal_total_w must be > 0")
        if phases is not None and phases not in (1, 3):
            raise ValueError("phases must be 1, 3, or None")
        if not (0.0 < quantile_q <= 1.0):
            raise ValueError("quantile_q must be in (0, 1].")
        if not (0.0 < alpha <= 1.0):
            raise ValueError("alpha must be in (0, 1].")

        self.nominal_total_w = float(nominal_total_w)
        self.phases: Optional[int] = phases

        self.window_s = float(window_s)
        self.quantile_q = float(quantile_q)
        self.alpha = float(alpha)

        self.expected_floor_w = float(expected_floor_w)
        self.expected_cap_mult = float(expected_cap_mult)

        self.significant_abs_w = float(significant_abs_w)
        self.significant_rel = float(significant_rel)

        # Track whether threshold was auto-derived so we can keep it consistent on nominal changes
        self._running_threshold_w_explicit = (running_threshold_w is not None)
        self.running_threshold_w = float(
            running_threshold_w
            if running_threshold_w is not None
            else max(0.25 * self.nominal_total_w, 600.0)
        )

        self._expected_total = _clamp(
            0.9 * self.nominal_total_w,
            self.expected_floor_w,
            self.expected_cap_mult * self.nominal_total_w
        )

        self._t = 0.0
        self._last_time: Optional[float] = None

        # Rolling window of (t, P_total) for running samples only
        self._run_hist: Deque[Tuple[float, float]] = deque()

        # For "significant change" detection
        self._last_reported_expected_total: Optional[float] = None

    # -------- public API --------

    @property
    def expected_P_total(self) -> float:
        return float(self._expected_total)

    def estimated_power(self) -> Power:
        """Return current per-phase estimate (no new input required)."""
        return self._split_expected_to_phases(self._expected_total)

    def set_nominal_total_w(self, nominal_total_w: float, *, mode: str = "auto", clear_history: bool = False) -> None:
        if nominal_total_w <= 0:
            raise ValueError("nominal_total_w must be > 0")

        new_nom = float(nominal_total_w)
        old_nom = float(self.nominal_total_w)
        self.nominal_total_w = new_nom

        if not self._running_threshold_w_explicit:
            self.running_threshold_w = max(0.25 * self.nominal_total_w, 600.0)

        learned = (len(self._run_hist) >= 20)  # same threshold you use to update expected

        if mode == "auto":
            mode = "rescale" if learned else "reanchor"

        if mode == "rescale":
            self._expected_total *= (new_nom / old_nom) if old_nom > 0 else 1.0
        elif mode == "reanchor":
            self._expected_total = 0.9 * new_nom
        elif mode == "clamp_only":
            pass
        else:
            raise ValueError("mode must be 'auto', 'rescale', 'reanchor', or 'clamp_only'")

        cap = self.expected_cap_mult * self.nominal_total_w
        self._expected_total = _clamp(self._expected_total, self.expected_floor_w, cap)

        if clear_history:
            self._run_hist.clear()
            self._last_reported_expected_total = self._expected_total

    def set_running_threshold_w(
        self,
        threshold_w: float | None,
        *,
        mode: str = "explicit",   # "explicit" | "auto"
        clear_history: bool = False,
    ) -> None:
        """
        Update the running threshold.

        mode:
        - "explicit": use threshold_w as fixed value
        - "auto": derive from nominal (ignores threshold_w)

        clear_history:
        - If True, clears running sample window (recommended if threshold changes a lot)
        """
        if mode == "auto":
            self._running_threshold_w_explicit = False
            self.running_threshold_w = max(0.25 * self.nominal_total_w, 600.0)
        elif mode == "explicit":
            if threshold_w is None or threshold_w <= 0:
                raise ValueError("threshold_w must be > 0 for explicit mode")
            self._running_threshold_w_explicit = True
            self.running_threshold_w = float(threshold_w)
        else:
            raise ValueError("mode must be 'explicit' or 'auto'")

        if clear_history:
            self._run_hist.clear()

    def feed(self, power: Power | None = None, *, L1=None, L2=None, L3=None) -> bool:
        """
        Feed a new sample. Returns True if the *expected estimate* changed significantly.
        Accepts either:
          - feed(power=Power(...))
          - feed(L1=..., L2=..., L3=...)  (compat)
        """
        if power is None:
            power = Power(
                None if L1 is None else int(L1),
                None if L2 is None else int(L2),
                None if L3 is None else int(L3),
            )

        now = time.monotonic()
        if self._last_time is None:
            self._last_time = now
            # initialize change baseline
            self._last_reported_expected_total = self._expected_total
            return False

        dt = now - self._last_time
        self._last_time = now
        if dt <= 0:
            dt = 0.001

        self._t += dt

        # Infer phases if not set: count "available" phases (non-None and non-zero)
        if self.phases is None:
            inferred = self._infer_phases_from_power(power)
            if inferred in (1, 3):
                self.phases = inferred

        p_total = self._estimate_total_power(power)

        # Only learn from running samples
        if p_total > self.running_threshold_w:
            self._run_hist.append((self._t, p_total))
            self._trim()

            if len(self._run_hist) >= 20:
                window_vals = [v for _, v in self._run_hist]
                target = _quantile(window_vals, self.quantile_q)

                new_expected = (1.0 - self.alpha) * self._expected_total + self.alpha * target
                cap = self.expected_cap_mult * self.nominal_total_w
                self._expected_total = _clamp(new_expected, self.expected_floor_w, cap)

        # significant change detection (on expected_total)
        changed = self._significant_change(self._expected_total)
        if changed:
            self._last_reported_expected_total = self._expected_total
        return changed

    # -------- internals --------

    def _infer_phases_from_power(self, power: Power) -> Optional[int]:
        present = sum(
            1 for v in (power.l1, power.l2, power.l3)
            if v is not None and v != 0
        )
        # only accept the two allowed values
        return present if present in (1, 3) else None

    def _estimate_total_power(self, power: Power) -> float:
        vals = [v for v in (power.l1, power.l2, power.l3) if v is not None and v != 0]
        if not vals:
            raise ValueError("At least one of L1/L2/L3 must be non-zero and not None.")
        vals_f = [float(v) for v in vals]
        total = sum(vals_f)

        # Balanced-phase scaling if phases known and some phases missing
        if self.phases and len(vals_f) < self.phases:
            total = (total / len(vals_f)) * self.phases

        return max(0.0, total)

    def _split_expected_to_phases(self, expected_total: float) -> Power:
        # If phases unknown, default to putting all into L1 (keeps it deterministic)
        if self.phases == 3:
            per = int(round(expected_total / 3.0))
            return Power(per, per, per)
        # phases == 1 or None
        return Power(int(round(expected_total)), None, None)

    def _trim(self) -> None:
        cutoff = self._t - self.window_s
        while self._run_hist and self._run_hist[0][0] < cutoff:
            self._run_hist.popleft()

    def _significant_change(self, new_expected: float) -> bool:
        old = self._last_reported_expected_total
        if old is None:
            return True
        delta = abs(new_expected - old)
        if delta >= self.significant_abs_w:
            return True
        # relative check against the larger of old/floor to avoid noisy % at low power
        denom = max(abs(old), self.expected_floor_w, 1.0)
        return (delta / denom) >= self.significant_rel