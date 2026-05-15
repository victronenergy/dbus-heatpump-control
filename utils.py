from __future__ import annotations

import time
import logging
from enum import IntEnum
from dataclasses import dataclass
from collections import deque
from typing import Callable, Deque, Tuple

from aiovelib.client import Service as ObservableService
from aiovelib.service import IntegerItem
from aiovelib.localsettings import Setting, SettingsService as ObservableSettingsService, SETTINGS_SERVICE


logger = logging.getLogger()


class SafeIntEnum(IntEnum):
    @classmethod
    def _missing_(cls, value):
        # Create a synthetic member with that value
        obj = int.__new__(cls, value)
        obj._name_ = f"UNKNOWN_{value}"
        obj._value_ = value
        return obj


class SERVICE_STATE(SafeIntEnum):
    OFF = 0
    ON = 1
    NOT_AVAILABLE = 2


class EnumItem(IntegerItem):
    def __init__(self, path, type, **kwargs):
        self.type = type
        super().__init__(path, **kwargs)

    def get_text(self):
        if self.value is None:
            return ''
        return self.type(self.value).name


class HeatpumpService(ObservableService):
    servicetype = "com.victronenergy.heatpump"
    paths = [
        "/DeviceInstance",
        "/Ac/L1/Power",
        "/Ac/L2/Power",
        "/Ac/L3/Power",
        "/NrOfPhases",
    ]

    @property
    def power(self):
        return Power(
            self.get_value("/Ac/L1/Power"),
            self.get_value("/Ac/L2/Power"),
            self.get_value("/Ac/L3/Power")
        )

    @property
    def phases(self):
        return self.get_value("/NrOfPhases")


class SystemService(ObservableService):
    servicetype = "com.victronenergy.system"
    paths = {
        "/SwitchableOutput/0/State",
        "/SwitchableOutput/0/Settings/Function",
        "/SwitchableOutput/1/State",
        "/SwitchableOutput/1/Settings/Function",
    }


class SettingsService(ObservableSettingsService):
    servicetype = SETTINGS_SERVICE
    paths = set()

    async def add_rm_settings(self,
                              ON_HYSTERESIS_S: int, OFF_HYSTERESIS_S: int,
                              POWER_SETTING_W: int, RUNNING_THRESH_W: int):
        await self.add_settings(
            Setting("/Settings/HeatpumpControl/OnHysteresis", ON_HYSTERESIS_S, _min=600),
			Setting("/Settings/HeatpumpControl/OffHysteresis", OFF_HYSTERESIS_S, _min=600),
            Setting("/Settings/HeatpumpControl/PowerSetting", POWER_SETTING_W, _min=0),
            Setting("/Settings/HeatpumpControl/RunningThreshold", RUNNING_THRESH_W, _min=0),
			Setting("/Settings/HeatpumpControl/EstimatedPowerOn", POWER_SETTING_W, _min=0),
            Setting("/Settings/HeatpumpControl/EstimatedPowerOff", 0, _min=0)
		)

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
        return any(v is not None for v in (self.l1, self.l2, self.l3))


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _quantile(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    # simple "nearest rank" quantile
    idx = max(0, min(len(s) - 1, int((len(s) - 1) * q)))
    return float(s[idx])


class RelayBlocked(RuntimeError):
    pass


class RELAY_STATE(IntEnum):
    OFF = 0
    ON = 1


@dataclass(frozen=True, slots=True)
class RelayConfig:
    required_function: int = 6  # block control unless Function == 6


class RelayChannel:
    """
    0-based relay channel view with:
      - function gate (only allow control if function == required_function)
      - raw function read + write
    """
    def __init__(self, index: int, system_service, settings_service, *, cfg: RelayConfig):
        self.index = int(index)
        self._system = system_service
        self._settings = settings_service
        self._cfg = cfg

    # ---- DBus paths ----

    def state_path(self) -> str:
        return f"/SwitchableOutput/{self.index}/State"

    def function_path(self) -> str:
        return f"/SwitchableOutput/{self.index}/Settings/Function"

    # ---- tidy IO helpers ----

    def _read_state_raw(self) -> int | None:
        v = self._system.get_value(self.state_path())
        return None if v is None else int(v)

    async def _write_state_raw(self, raw: int) -> None:
        await self._system.set_value(self.state_path(), int(raw))

    def _read_function(self) -> int | None:
        v = self._system.get_value(self.function_path())
        return None if v is None else int(v)

    async def _write_function(self, raw: int) -> None:
        await self._system.set_value(self.function_path(), int(raw))

    # ---- public raw access ----

    @property
    def function_raw(self) -> int | None:
        return self._read_function()

    async def set_function_raw(self, raw: int) -> None:
        await self._write_function(raw)

    # ---- gating + logical state ----

    @property
    def controllable(self) -> bool:
        f = self._read_function()
        return (f is not None) and (f == self._cfg.required_function)

    @property
    def state(self) -> RELAY_STATE | None:
        raw = self._read_state_raw()
        if raw is None:
            return None
        return RELAY_STATE.ON if raw else RELAY_STATE.OFF

    async def set_state(self, state: RELAY_STATE) -> None:
        if not self.controllable:
            raise RuntimeError(f"Relay {self.index} blocked: Function != {self._cfg.required_function}")
        await self._write_state_raw(int(state))


class Relays:
    """0-based relay collection: relays[0], relays[1], ..."""
    def __init__(self, system_service, settings_service, *, count: int = 2, cfg: RelayConfig = RelayConfig()):
        self._channels = [RelayChannel(i, system_service, settings_service, cfg=cfg) for i in range(int(count))]

    def __getitem__(self, idx: int) -> RelayChannel:
        return self._channels[int(idx)]

    def __len__(self) -> int:
        return len(self._channels)


class HpItems:
    def __init__(self, svc, settings: SettingsService):
        self.svc = svc
        self.settings = settings

    def get_int(self, path: str, default: int) -> int:
        it = self.svc.get_item(path)
        return int(it.value) if it and it.value is not None else int(default)

    def set_local(self, path: str, value) -> None:
        it = self.svc.get_item(path)
        if it:
            it.set_local_value(value)

    def get_setting(self, path: str) -> int:
        return self.settings.get_value(path)

    def set_setting(self, path: str, val: int):
        self.settings.set_value_async(path, val)

    @property
    def s2_active(self) -> int:
        return self.get_int("/S2/0/Active", 0)

    @s2_active.setter
    def s2_active(self, v: int):
        self.set_local("/S2/0/Active", int(v))

    @property
    def on_hysteresis(self) -> int:
        return self.get_setting("/Settings/HeatpumpControl/OnHysteresis")

    @on_hysteresis.setter
    def on_hysteresis(self, val: int):
        self.set_setting("/Settings/HeatpumpControl/OnHysteresis", val)
        self.set_local("/S2/0/RmSettings/OnHysteresis", val)

    @property
    def off_hysteresis(self) -> int:
        return self.get_setting("/Settings/HeatpumpControl/OffHysteresis")

    @off_hysteresis.setter
    def off_hysteresis(self, val: int):
        self.set_setting("/Settings/HeatpumpControl/OffHysteresis", val)
        self.set_local("/S2/0/RmSettings/OffHysteresis", val)

    @property
    def power_setting(self) -> int:
        return self.get_setting("/Settings/HeatpumpControl/PowerSetting")

    @power_setting.setter
    def power_setting(self, val: int):
        self.set_setting("/Settings/HeatpumpControl/PowerSetting", val)
        self.set_local("/S2/0/RmSettings/PowerSetting", val)

    @property
    def running_threshold(self) -> int:
        return self.get_setting("/Settings/HeatpumpControl/RunningThreshold")

    @running_threshold.setter
    def running_threshold(self, val: int):
        self.set_setting("/Settings/HeatpumpControl/RunningThreshold", val)
        self.set_local("/S2/0/RmSettings/RunningThreshold", val)

    @property
    def current_power(self) -> int | None:
        it = self.svc.get_item("/Ac/Power")
        return int(it.value) if it and it.value is not None else None

    @current_power.setter
    def current_power(self, v: int | None):
        self.set_local("/Ac/Power", v)

    @property
    def estimated_power_on(self) -> int:
        return self.get_setting("/Settings/HeatpumpControl/EstimatedPowerOn")

    @estimated_power_on.setter
    def estimated_power_on(self, v: int):
        self.set_setting("/Settings/HeatpumpControl/EstimatedPowerOn", int(v))
        self.set_local("/EstimatedPowerOn", int(v))

    @property
    def estimated_power_off(self) -> int:
        return self.get_setting("/Settings/HeatpumpControl/EstimatedPowerOff")

    @estimated_power_off.setter
    def estimated_power_off(self, v: int):
        self.set_setting("/Settings/HeatpumpControl/EstimatedPowerOff", int(v))
        self.set_local("/EstimatedPowerOff", int(v))

    @property
    def state(self) -> SERVICE_STATE:
        i = self.get_int("/State", 2)
        return SERVICE_STATE(i)

    @state.setter
    def state(self, v: int) :
        i = SERVICE_STATE.NOT_AVAILABLE
        if self.svc._relay_function_ok():
            i = SERVICE_STATE(v)
        self.set_local("/State",i)


class EstimatorManager:
    def __init__(self, estimator_factory: Callable[..., object], **estimator_kwargs):
        self._make = estimator_factory
        self._kwargs = dict(estimator_kwargs)
        self.est = None
        self.hp_phases: int | None = None

    def init(self, *, nominal_w: int, phases: int | None, running_thr: int):
        self.hp_phases = phases if phases in (1, 3) else None
        self.est = self._make(
            nominal_total_w=nominal_w,
            phases=self.hp_phases,
            running_threshold_w=running_thr,
            **self._kwargs,
        )

    def feed(self, power) -> bool:
        return bool(self.est.feed(power)) if self.est else False

    def estimated_total(self) -> int:
        if not self.est:
            return 0
        return int(self.est.estimated_power().total)

    def set_nominal(self, nominal_w: int, *, mode: str = "auto", clear_history: bool = True):
        if not self.est:
            return
        self.est.set_nominal_total_w(nominal_w, mode=mode, clear_history=clear_history)

    def set_running_threshold(self, thr_w: int, *, clear_history: bool = True):
        if not self.est:
            return
        self.est.set_running_threshold_w(thr_w, mode="explicit", clear_history=clear_history)

    def set_phases(self, phases: int, *, keep_expected: bool = True) -> bool:
        if phases not in (1, 3):
            return False
        if phases == self.hp_phases:
            return False
        self.hp_phases = phases
        if not self.est:
            return True
        self.est = self.est.recreate(phases=phases, keep_expected=keep_expected)
        return True


class HeatpumpPowerEstimator:
    """
    Expected running power estimator.

      - Consider HP "running" when P_total > running_threshold_w
      - Keep a rolling window of running samples (time-based)
    - target from rolling window (quantile or mean)
      - expected_P_total = EWMA toward target
      - per-phase estimate derived from expected total
    """

    def __init__(
        self,
        nominal_total_w: float,
        phases: int | None = None,
        *,
        running_threshold_w: float | None = None,
        window_s: float = 600.0,
        quantile_q: float = 0.75,
        alpha: float = 0.05,
        expected_floor_w: float = 300.0,
        expected_cap_mult: float | None = None,
        target_mode: str = "quantile",  # "quantile" | "mean"
        min_samples: int = 20,

        # "significant change" thresholds for feed() return value
        significant_abs_w: float = 25.0,
        significant_rel: float = 0.10,
        learn_when_running: bool = True,
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
        self.phases: int | None = phases

        self.window_s = float(window_s)
        self.quantile_q = float(quantile_q)
        self.alpha = float(alpha)

        self.expected_floor_w = expected_floor_w
        self.expected_cap_mult = expected_cap_mult
        self.target_mode = str(target_mode)
        if self.target_mode not in ("quantile", "mean"):
            raise ValueError("target_mode must be 'quantile' or 'mean'")
        self.min_samples = int(min_samples)
        if self.min_samples < 1:
            raise ValueError("min_samples must be >= 1")

        self.significant_abs_w = float(significant_abs_w)
        self.significant_rel = float(significant_rel)
        self.learn_when_running = bool(learn_when_running)

        # Track whether threshold was auto-derived so we can keep it consistent on nominal changes
        self._running_threshold_w_explicit = (running_threshold_w is not None)
        self.running_threshold_w = float(
            running_threshold_w
            if running_threshold_w is not None
            else max(0.25 * self.nominal_total_w, 600.0)
        )

        new_expected = self.nominal_total_w
        if self.expected_cap_mult is None:
            self._expected_total = max(self.expected_floor_w, new_expected)
        else:
            cap = self.expected_cap_mult * self.nominal_total_w
            self._expected_total = _clamp(new_expected, self.expected_floor_w, cap)

        self._t = 0.0
        self._last_time: float | None = None

        # Rolling window of (t, P_total) for running samples only
        self._run_hist: Deque[Tuple[float, float]] = deque()

        # For "significant change" detection
        self._last_reported_expected_total: float | None = None

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

        learned = (len(self._run_hist) >= 20)

        if mode == "auto":
            mode = "rescale" if learned else "reanchor"

        if mode == "rescale":
            self._expected_total *= (new_nom / old_nom) if old_nom > 0 else 1.0
        elif mode == "reanchor":
            self._expected_total = new_nom
        elif mode == "clamp_only":
            pass
        else:
            raise ValueError("mode must be 'auto', 'rescale', 'reanchor', or 'clamp_only'")

        new_expected = self._expected_total
        if self.expected_cap_mult is None:
            self._expected_total = max(self.expected_floor_w, new_expected)
        else:
            cap = self.expected_cap_mult * self.nominal_total_w
            self._expected_total = _clamp(new_expected, self.expected_floor_w, cap)

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

        should_learn = (
            p_total > self.running_threshold_w
            if self.learn_when_running
            else p_total <= self.running_threshold_w
        )

        # Learn from the selected sample class (running or idle)
        if should_learn:
            self._run_hist.append((self._t, p_total))
            self._trim()

            if len(self._run_hist) >= self.min_samples:
                window_vals = [v for _, v in self._run_hist]
                target = self._target_from_window(window_vals)

                new_expected = (1.0 - self.alpha) * self._expected_total + self.alpha * target
                if self.expected_cap_mult is None:
                    self._expected_total = max(self.expected_floor_w, new_expected)
                else:
                    cap = self.expected_cap_mult * self.nominal_total_w
                    self._expected_total = _clamp(new_expected, self.expected_floor_w, cap)

        # significant change detection (on expected_total)
        changed = self._significant_change(self._expected_total)
        if changed:
            self._last_reported_expected_total = self._expected_total
        return changed

    def recreate(self, *, phases: int | None, keep_expected: bool = True) -> "HeatpumpPowerEstimator":
        """
        Return a fresh estimator with the same configuration but different phases.
        History is cleared by definition.

        keep_expected:
        - True: carry over current expected total (clamped to new nominal/cap)
        - False: re-anchor to nominal (default constructor behavior)
        """
        new = HeatpumpPowerEstimator(
            nominal_total_w=self.nominal_total_w,
            phases=phases,
            running_threshold_w=(self.running_threshold_w if self._running_threshold_w_explicit else None),
            window_s=self.window_s,
            quantile_q=self.quantile_q,
            alpha=self.alpha,
            expected_floor_w=self.expected_floor_w,
            expected_cap_mult=self.expected_cap_mult,
            target_mode=self.target_mode,
            min_samples=self.min_samples,
            significant_abs_w=self.significant_abs_w,
            significant_rel=self.significant_rel,
            learn_when_running=self.learn_when_running,
        )

        # preserve explicit/auto threshold behavior
        new._running_threshold_w_explicit = self._running_threshold_w_explicit
        if not new._running_threshold_w_explicit:
            new.running_threshold_w = max(0.25 * new.nominal_total_w, 600.0)

        if keep_expected:

            new_expected = self._expected_total
            if self.expected_cap_mult is None:
                new._expected_total = max(self.expected_floor_w, new_expected)
            else:
                cap = self.expected_cap_mult * self.nominal_total_w
                new._expected_total = _clamp(new_expected, self.expected_floor_w, cap)

        return new

    # -------- internals --------

    def _infer_phases_from_power(self, power: Power) -> int | None:
        present = sum(
            1 for v in (power.l1, power.l2, power.l3)
            if v is not None and v != 0
        )
        # only accept the two allowed values
        return present if present in (1, 3) else None

    def _estimate_total_power(self, power: Power) -> float:
        vals = [v for v in (power.l1, power.l2, power.l3) if v is not None]
        if not vals:
            raise ValueError("At least one of L1/L2/L3 must be not None.")
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

    def _target_from_window(self, values: list[float]) -> float:
        if self.target_mode == "mean":
            return float(sum(values) / len(values)) if values else 0.0
        return _quantile(values, self.quantile_q)

    def _significant_change(self, new_expected: float) -> bool:
        old = self._last_reported_expected_total
        if old is None:
            return True
        delta = new_expected - old

        if abs(delta) >= self.significant_abs_w:
            return True
        # relative check against the larger of old/floor to avoid noisy % at low power
        denom = max(abs(old), self.expected_floor_w, 1.0)
        return abs(delta / denom) >= self.significant_rel
