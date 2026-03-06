#!/usr/bin/python3
"""
com.victronenergy.heatpump.mock
"""

from __future__ import annotations

import os
import sys
import math
import time
import asyncio
import logging
import random
from argparse import ArgumentParser

from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "ext", "aiovelib"))
from aiovelib.service import Service, IntegerItem, DoubleItem, TextItem
from aiovelib.client import Monitor, Service as ObservableService, ServiceHandler

logger = logging.getLogger(__name__)


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


class SystemService(ObservableService, ServiceHandler):
    servicetype = "com.victronenergy.system"
    paths = [
        "/SwitchableOutput/0/Settings/Function",
        "/SwitchableOutput/0/State",
        "/SwitchableOutput/1/Settings/Function",
        "/SwitchableOutput/1/State",
    ]


class RelayMonitor(Monitor):
    def __init__(self, *args, heatpump_service: "HeatpumpTestService", relay_index: int, **kwargs):
        super().__init__(*args, **kwargs)
        self._heatpump_service = heatpump_service
        self._relay_index = int(relay_index)

    async def _sync_selected_relay(self, service) -> None:
        fn_path = f"/SwitchableOutput/{self._relay_index}/Settings/Function"
        st_path = f"/SwitchableOutput/{self._relay_index}/State"

        fn = self.get_value(SystemService.servicetype, fn_path)
        st = self.get_value(SystemService.servicetype, st_path)

        await self._heatpump_service.set_sgready_state(fn == 6 and st == 1)

    def itemsChanged(self, service, values):
        relevant = {
            f"/SwitchableOutput/{self._relay_index}/Settings/Function",
            f"/SwitchableOutput/{self._relay_index}/State",
        }

        if any(path in relevant for path in values.keys()):
            asyncio.create_task(self._sync_selected_relay(service))


class HeatpumpTestService(Service):
    SERVICE_NAME = "com.victronenergy.heatpump.mock"

    V_AC = 230.0
    PF_ON = 0.95
    PF_OFF = 0.0

    # legacy auto mode
    ON_S = 12 * 60
    OFF_S = 6 * 60
    PERIOD_S = ON_S + OFF_S

    DEFAULT_NOMINAL_W = 2000.0
    DEFAULT_SPIKINESS = 4  # 0..100
    UPDATE_DT = 1.0

    # sgready defaults
    DEFAULT_MODE = "auto"

    DEFAULT_HP_HEATING_RATE_C_PER_H = 12.0
    DEFAULT_TEMP_LOW_C = 30.0
    DEFAULT_TEMP_NORMAL_SETPOINT_C = 60.0
    DEFAULT_TEMP_SGREADY_SETPOINT_C = 75.0
    DEFAULT_HYSTERESIS_C = 5.0
    DEFAULT_START_TEMP_C = 50.0

    DEFAULT_MIN_RUNTIME_S = 10 * 60
    DEFAULT_MIN_STOPPED_S = 10 * 60

    # tuned so 60C -> 33C is around 8h on average without heating
    DEFAULT_LEAKAGE_BASE_C_PER_H = 2.2
    DEFAULT_LEAKAGE_USE_C_PER_H = 1.2
    DEFAULT_LEAKAGE_EVENT_INTERVAL_S = 45 * 60
    DEFAULT_LEAKAGE_EVENT_JITTER_S = 20 * 60
    DEFAULT_LEAKAGE_EVENT_DROP_C_MIN = 0.2
    DEFAULT_LEAKAGE_EVENT_DROP_C_MAX = 1.2

    def __init__(
        self,
        bus,
        *,
        mode: str = DEFAULT_MODE,
        interactive: bool = False,
        nominal_w: float = DEFAULT_NOMINAL_W,
        spikiness: int = DEFAULT_SPIKINESS,
        hp_heating_rate_c_per_h: float = DEFAULT_HP_HEATING_RATE_C_PER_H,
        temp_low_c: float = DEFAULT_TEMP_LOW_C,
        temp_normal_setpoint_c: float = DEFAULT_TEMP_NORMAL_SETPOINT_C,
        temp_sgready_setpoint_c: float = DEFAULT_TEMP_SGREADY_SETPOINT_C,
        hysteresis_c: float = DEFAULT_HYSTERESIS_C,
        start_temp_c: float = DEFAULT_START_TEMP_C,
        min_runtime_s: float = DEFAULT_MIN_RUNTIME_S,
        min_stopped_s: float = DEFAULT_MIN_STOPPED_S,
        leakage_base_c_per_h: float = DEFAULT_LEAKAGE_BASE_C_PER_H,
        leakage_use_c_per_h: float = DEFAULT_LEAKAGE_USE_C_PER_H,
        leakage_event_interval_s: float = DEFAULT_LEAKAGE_EVENT_INTERVAL_S,
        leakage_event_jitter_s: float = DEFAULT_LEAKAGE_EVENT_JITTER_S,
        leakage_event_drop_c_min: float = DEFAULT_LEAKAGE_EVENT_DROP_C_MIN,
        leakage_event_drop_c_max: float = DEFAULT_LEAKAGE_EVENT_DROP_C_MAX,
        relay_index: int | None = None,
    ):
        super().__init__(bus=bus, name=self.SERVICE_NAME)

        if interactive:
            mode = "interactive"
        if mode not in ("auto", "interactive", "sgready"):
            raise ValueError("mode must be one of: auto, interactive, sgready")

        self._mode = mode
        self._interactive = (mode == "interactive")

        self._t0 = time.monotonic()
        self._last = self._t0

        self._l1_power_w = 0.0
        self._e_fwd_kwh_total = 0.0
        self._e_fwd_kwh_l1 = 0.0
        self._e_rev_kwh_total = 0.0
        self._e_rev_kwh_l1 = 0.0

        # generic controls
        self._forced_on = False
        self._nominal_w = float(nominal_w)
        self._spikiness = int(_clamp(float(spikiness), 0.0, 100.0))

        # sgready model parameters
        self._hp_heating_rate_c_per_h = float(hp_heating_rate_c_per_h)
        self._temp_low_c = float(temp_low_c)
        self._temp_normal_setpoint_c = float(temp_normal_setpoint_c)
        self._temp_sgready_setpoint_c = float(temp_sgready_setpoint_c)
        self._hysteresis_c = float(hysteresis_c)
        self._start_temp_c = float(start_temp_c)
        self._min_runtime_s = float(min_runtime_s)
        self._min_stopped_s = float(min_stopped_s)

        self._leakage_base_c_per_h = float(leakage_base_c_per_h)
        self._leakage_use_c_per_h = float(leakage_use_c_per_h)
        self._leakage_event_interval_s = float(leakage_event_interval_s)
        self._leakage_event_jitter_s = float(leakage_event_jitter_s)
        self._leakage_event_drop_c_min = float(leakage_event_drop_c_min)
        self._leakage_event_drop_c_max = float(leakage_event_drop_c_max)

        # sgready state
        self._excess_pv = False
        self._tank_temp_c = self._start_temp_c
        self._hp_running = False
        self._hp_last_switch_ts = self._t0 - self._min_stopped_s
        self._next_leak_event_ts = self._schedule_next_leak_event(self._t0)

        # blocking timers
        self._min_runtime_remaining_s = 0
        self._min_stopped_remaining_s = 0

        self._relay_index = relay_index

        self._lock = asyncio.Lock()

    # ------------------------------------------------------------
    # Public control API used by CLI / external integration
    # ------------------------------------------------------------

    async def set_on(self, on: bool) -> None:
        async with self._lock:
            self._forced_on = bool(on)

    async def set_nominal_power(self, watts: float) -> None:
        w = float(watts)
        if w <= 0:
            raise ValueError("nominal power must be > 0")
        async with self._lock:
            self._nominal_w = w

    async def set_spikiness(self, spikiness: int) -> None:
        s = int(spikiness)
        if s < 0 or s > 100:
            raise ValueError("spikiness must be 0..100")
        async with self._lock:
            self._spikiness = s

    def _update_sgready_state(self, v) -> None:
        asyncio.create_task(self.set_sgready_state(True if v == 1 else False))

    async def set_sgready_state(self, excess_pv: bool) -> None:
        async with self._lock:
            self._excess_pv = bool(excess_pv)

    async def get_status(self) -> dict:
        async with self._lock:
            active_setpoint = self._get_active_setpoint_c()
            return {
                "mode": self._mode,
                "forced_on": self._forced_on,
                "nominal_w": self._nominal_w,
                "spikiness": self._spikiness,
                "power_w": float(self._l1_power_w),
                "energy_fwd_kwh": float(self._e_fwd_kwh_total),
                "sgready": {
                    "excess_pv": self._excess_pv,
                    "tank_temp_c": float(self._tank_temp_c),
                    "active_setpoint_c": float(active_setpoint),
                    "hp_running": self._hp_running,
                    "hysteresis_c": self._hysteresis_c,
                    "min_runtime_remaining_s": int(self._min_runtime_remaining_s),
                    "min_stopped_remaining_s": int(self._min_stopped_remaining_s),
                },
            }

    # ------------------------------------------------------------

    def _is_on_auto(self, t: float) -> bool:
        ph = t % self.PERIOD_S
        return ph < self.ON_S

    def _noise(self, amp: float) -> float:
        return (2.0 * (os.urandom(1)[0] / 255.0) - 1.0) * amp

    def _noise_amp(self, nominal_w: float, spikiness: int) -> float:
        """
        spikiness: 0..100
          0   -> 0% of nominal
          100 -> 10% of nominal
        """
        frac = (float(spikiness) / 100.0) * 0.10
        return max(0.0, nominal_w * frac)

    def _lowpass(self, cur: float, tgt: float, tau_s: float, dt: float) -> float:
        a = 1.0 - math.exp(-dt / tau_s)
        return cur + a * (tgt - cur)

    def _calc_current(self, p_w: float, pf: float) -> float:
        if p_w <= 0 or pf <= 0:
            return 0.0
        return p_w / (self.V_AC * pf)

    def _kwh_add(self, p_w: float, dt_s: float) -> float:
        return (p_w * dt_s) / 3_600_000.0

    def _set_local(self, path: str, value):
        it = self.get_item(path)
        if it:
            it.set_local_value(value)

    # ------------------------------------------------------------
    # SG Ready model
    # ------------------------------------------------------------

    def _get_active_setpoint_c(self) -> float:
        return self._temp_sgready_setpoint_c if self._excess_pv else self._temp_normal_setpoint_c

    def _schedule_next_leak_event(self, now: float) -> float:
        jitter = random.uniform(-self._leakage_event_jitter_s, self._leakage_event_jitter_s)
        return now + max(60.0, self._leakage_event_interval_s + jitter)

    def _apply_sgready_leakage(self, now: float, dt: float) -> None:
        # continuous standing losses + abstract household usage
        leak_rate_c_per_h = self._leakage_base_c_per_h + self._leakage_use_c_per_h
        self._tank_temp_c -= leak_rate_c_per_h * (dt / 3600.0)

        # randomized household draw events
        if now >= self._next_leak_event_ts:
            drop = random.uniform(self._leakage_event_drop_c_min, self._leakage_event_drop_c_max)
            self._tank_temp_c -= drop
            self._next_leak_event_ts = self._schedule_next_leak_event(now)

        # don't allow tank to drift far below low state
        self._tank_temp_c = max(self._temp_low_c - 2.0, self._tank_temp_c)

    def _apply_sgready_heating(self, dt: float) -> None:
        if self._hp_running:
            self._tank_temp_c += self._hp_heating_rate_c_per_h * (dt / 3600.0)

    def _update_sgready_control(self, now: float) -> bool:
        setpoint = self._get_active_setpoint_c()
        start_below = setpoint - self._hysteresis_c
        stop_at_or_above = setpoint

        elapsed = now - self._hp_last_switch_ts

        if self._hp_running:
            self._min_runtime_remaining_s = max(0, int(math.ceil(self._min_runtime_s - elapsed)))
            self._min_stopped_remaining_s = 0

            if self._tank_temp_c >= stop_at_or_above and elapsed >= self._min_runtime_s:
                self._hp_running = False
                self._hp_last_switch_ts = now
                self._min_runtime_remaining_s = 0
                self._min_stopped_remaining_s = int(math.ceil(self._min_stopped_s))

        else:
            self._min_runtime_remaining_s = 0
            self._min_stopped_remaining_s = max(0, int(math.ceil(self._min_stopped_s - elapsed)))

            if self._tank_temp_c <= start_below and elapsed >= self._min_stopped_s:
                self._hp_running = True
                self._hp_last_switch_ts = now
                self._min_stopped_remaining_s = 0
                self._min_runtime_remaining_s = int(math.ceil(self._min_runtime_s))

        return self._hp_running

    def _sgready_target_power(self, on: bool, nominal: float, spk: int) -> float:
        noise_amp = self._noise_amp(nominal, spk)
        if on:
            return nominal + self._noise(noise_amp)
        return 0.0 + self._noise(5.0)

    # ------------------------------------------------------------

    async def register(self):
        # Totals
        self.add_item(DoubleItem("/Ac/Energy/Forward", 0.0, text=lambda v: f"{v:.3f} kWh"))
        self.add_item(DoubleItem("/Ac/Energy/Reverse", 0.0, text=lambda v: f"{v:.3f} kWh"))
        self.add_item(IntegerItem("/Ac/Power", 0, text=lambda v: f"{v:.0f} W"))
        self.add_item(DoubleItem("/Ac/PowerFactor", 0.0, text=lambda v: f"{v:.2f}"))

        # Deprecated totals
        self.add_item(DoubleItem("/Ac/Current", 0.0, text=lambda v: f"{v:.2f} A"))
        self.add_item(DoubleItem("/Ac/Voltage", self.V_AC, text=lambda v: f"{v:.1f} V"))

        # L1
        self.add_item(DoubleItem("/Ac/L1/Current", 0.0, text=lambda v: f"{v:.2f} A"))
        self.add_item(DoubleItem("/Ac/L1/Energy/Forward", 0.0, text=lambda v: f"{v:.3f} kWh"))
        self.add_item(DoubleItem("/Ac/L1/Energy/Reverse", 0.0, text=lambda v: f"{v:.3f} kWh"))
        self.add_item(IntegerItem("/Ac/L1/Power", 0, text=lambda v: f"{v:.0f} W"))
        self.add_item(DoubleItem("/Ac/L1/PowerFactor", 0.0, text=lambda v: f"{v:.2f}"))
        self.add_item(DoubleItem("/Ac/L1/Voltage", self.V_AC, text=lambda v: f"{v:.1f} V"))

        # L2/L3 fixed
        for ph in ("L2", "L3"):
            self.add_item(DoubleItem(f"/Ac/{ph}/Current", 0.0, text=lambda v: f"{v:.2f} A"))
            self.add_item(DoubleItem(f"/Ac/{ph}/Energy/Forward", 0.0, text=lambda v: f"{v:.3f} kWh"))
            self.add_item(DoubleItem(f"/Ac/{ph}/Energy/Reverse", 0.0, text=lambda v: f"{v:.3f} kWh"))
            self.add_item(IntegerItem(f"/Ac/{ph}/Power", 0, text=lambda v: f"{v:.0f} W"))
            self.add_item(DoubleItem(f"/Ac/{ph}/PowerFactor", 0.0, text=lambda v: f"{v:.2f}"))
            self.add_item(DoubleItem(f"/Ac/{ph}/Voltage", self.V_AC, text=lambda v: f"{v:.1f} V"))

        self.add_item(IntegerItem("/NrOfPhases", 1))
        self.add_item(IntegerItem("/IsGenericEnergyMeter", 1))

        # Misc
        self.add_item(TextItem("/ProductName", "Heat Pump Mock"))
        self.add_item(TextItem("/CustomName", f"Heat Pump Mock ({self._mode})"))
        self.add_item(TextItem("/Mgmt/Connection", "D-Bus"))
        self.add_item(TextItem("/Mgmt/ProcessName", "heatpump.py"))
        self.add_item(TextItem("/Mgmt/ProcessVersion", "-"))
        self.add_item(IntegerItem("/Connected", 1))
        self.add_item(IntegerItem("/DeviceInstance", 200))
        self.add_item(IntegerItem("/ProductId", 0))
        self.add_item(IntegerItem("/Serial", '0000000000000'))
        self.add_item(IntegerItem("/HardwareVersion", '1'))
        self.add_item(IntegerItem("/FirmwareVersion", '1'))
        self.add_item(IntegerItem("/ErrorCode", 0))
        self.add_item(IntegerItem("/Position", 1))

        # sgready observability
        if self._mode == "sgready":
            self.add_item(DoubleItem("/Tank/Temperature", self._tank_temp_c, text=lambda v: f"{v:.1f} C"))
            self.add_item(DoubleItem("/Tank/Setpoint", self._get_active_setpoint_c(), text=lambda v: f"{v:.1f} C"))
            self.add_item(IntegerItem("/SgReady/ExcessPv", 1 if self._excess_pv else 0,
                                    writeable=True, onchange=self._update_sgready_state))
            self.add_item(IntegerItem("/SgReady/Running", 1 if self._hp_running else 0))
            self.add_item(IntegerItem("/SgReady/MinRuntimeRemaining", 0, text=lambda v: f"{v:d} s"))
            self.add_item(IntegerItem("/SgReady/MinStoppedRemaining", 0, text=lambda v: f"{v:d} s"))

        await super().register()
        asyncio.create_task(self._run())

    # ------------------------------------------------------------

    async def _run(self):
        while True:
            now = time.monotonic()
            dt = max(0.001, now - self._last)
            self._last = now
            t = now - self._t0

            async with self._lock:
                nominal = self._nominal_w
                spk = self._spikiness

                if self._mode == "interactive":
                    on = self._forced_on

                elif self._mode == "auto":
                    on = self._is_on_auto(t)

                else:  # sgready
                    self._apply_sgready_leakage(now, dt)
                    self._update_sgready_control(now)
                    self._apply_sgready_heating(dt)
                    self._tank_temp_c = min(self._temp_sgready_setpoint_c + 1.0, self._tank_temp_c)
                    on = self._hp_running

                if self._mode == "sgready":
                    tgt = self._sgready_target_power(on, nominal, spk)
                else:
                    noise_amp = self._noise_amp(nominal, spk)
                    if on:
                        tgt = nominal + self._noise(noise_amp)
                    else:
                        tgt = 0.0 + self._noise(5.0)

            self._l1_power_w = self._lowpass(self._l1_power_w, tgt, tau_s=20.0, dt=dt)

            if self._l1_power_w < 5.0:
                self._l1_power_w = 0.0

            p = int(round(self._l1_power_w))
            pf = self.PF_ON if p > 0 else self.PF_OFF
            i = self._calc_current(float(p), pf)

            dkwh = self._kwh_add(float(p), dt)
            self._e_fwd_kwh_l1 += dkwh
            self._e_fwd_kwh_total += dkwh

            # Totals from L1
            self._set_local("/Ac/L1/Power", p)
            self._set_local("/Ac/Power", p)
            self._set_local("/Ac/L1/PowerFactor", pf)
            self._set_local("/Ac/PowerFactor", pf)
            self._set_local("/Ac/L1/Current", i)
            self._set_local("/Ac/Current", i)

            self._set_local("/Ac/Energy/Forward", self._e_fwd_kwh_total)
            self._set_local("/Ac/Energy/Reverse", self._e_rev_kwh_total)
            self._set_local("/Ac/L1/Energy/Forward", self._e_fwd_kwh_l1)
            self._set_local("/Ac/L1/Energy/Reverse", self._e_rev_kwh_l1)

            for ph in ("L2", "L3"):
                self._set_local(f"/Ac/{ph}/Power", 0)
                self._set_local(f"/Ac/{ph}/PowerFactor", 0.0)
                self._set_local(f"/Ac/{ph}/Current", 0.0)
                self._set_local(f"/Ac/{ph}/Energy/Forward", 0.0)
                self._set_local(f"/Ac/{ph}/Energy/Reverse", 0.0)

            # sgready observability
            self._set_local("/Tank/Temperature", self._tank_temp_c)
            self._set_local("/Tank/Setpoint", self._get_active_setpoint_c())
            self._set_local("/SgReady/ExcessPv", 1 if self._excess_pv else 0)
            self._set_local("/SgReady/Running", 1 if (self._mode == "sgready" and self._hp_running) else 0)
            self._set_local("/SgReady/MinRuntimeRemaining", int(self._min_runtime_remaining_s))
            self._set_local("/SgReady/MinStoppedRemaining", int(self._min_stopped_remaining_s))

            await asyncio.sleep(self.UPDATE_DT)

    # ------------------------------------------------------------
    # CLI
    # ------------------------------------------------------------

    async def interactive_cli(self):
        print("Interactive mode.")
        print("Commands: on | off | set <watts> | spike <0-100> | sg on | sg off | status | help | quit")

        loop = asyncio.get_running_loop()

        while True:
            raw = await loop.run_in_executor(None, input, "hp> ")
            line = raw.strip()
            if not line:
                continue

            parts = line.split()
            cmd = parts[0].lower()

            try:
                if cmd == "on":
                    await self.set_on(True)
                    print("Heatpump ON")

                elif cmd == "off":
                    await self.set_on(False)
                    print("Heatpump OFF")

                elif cmd == "set":
                    if len(parts) != 2:
                        print("Usage: set <watts>")
                        continue
                    w = float(parts[1])
                    await self.set_nominal_power(w)
                    print(f"Nominal power set to {w:.0f} W")

                elif cmd in ("spike", "spikiness"):
                    if len(parts) != 2:
                        print("Usage: spike <0-100>")
                        continue
                    s = int(parts[1])
                    await self.set_spikiness(s)
                    print(f"Spikiness set to {s} (noise up to ~{s/10:.1f}% of nominal)")

                elif cmd == "sg":
                    if len(parts) != 2 or parts[1].lower() not in ("on", "off"):
                        print("Usage: sg on | sg off")
                        continue
                    state = parts[1].lower() == "on"
                    await self.set_sgready_state(state)
                    print(f"SG Ready excess PV set to {state}")

                elif cmd == "status":
                    st = await self.get_status()
                    msg = (
                        f"Mode={st['mode']} ForcedOn={st['forced_on']} "
                        f"Nominal={st['nominal_w']:.0f}W Spikiness={st['spikiness']} "
                        f"Power={st['power_w']:.0f}W EnergyFwd={st['energy_fwd_kwh']:.3f}kWh"
                    )
                    if st["mode"] == "sgready":
                        sg = st["sgready"]
                        msg += (
                            f" Temp={sg['tank_temp_c']:.1f}C"
                            f" Setpoint={sg['active_setpoint_c']:.1f}C"
                            f" ExcessPV={sg['excess_pv']}"
                            f" Running={sg['hp_running']}"
                        )
                    print(msg)

                elif cmd in ("help", "?"):
                    print("Commands:")
                    print("  on             - force heatpump on")
                    print("  off            - force heatpump off")
                    print("  set <watts>    - set nominal power (W)")
                    print("  spike <0-100>  - set spikiness (noise amplitude)")
                    print("  sg on|off      - set SG Ready excess PV signal")
                    print("  status         - show current state")
                    print("  quit / exit    - terminate")

                elif cmd in ("quit", "exit"):
                    print("Exiting...")
                    os._exit(0)

                else:
                    print("Unknown command. Type 'help'.")

            except Exception as e:
                print(f"Error: {e}")


async def main():
    parser = ArgumentParser()
    parser.add_argument("--dbus", default="system", help="system or session (default: system)")
    parser.add_argument("--interactive", "-i", action="store_true", help="manual CLI mode")
    parser.add_argument("--mode", choices=("auto", "interactive", "sgready"), default=HeatpumpTestService.DEFAULT_MODE,
                        help="simulation mode (default: auto)")
    parser.add_argument("--nominal", type=float, default=HeatpumpTestService.DEFAULT_NOMINAL_W,
                        help="nominal power in W (default: 2000)")
    parser.add_argument("--spikiness", type=int, default=HeatpumpTestService.DEFAULT_SPIKINESS,
                        help="0..100, noise amplitude up to ~10%% of nominal (default: 4)")
    parser.add_argument('--relay', help='Define GX relay to be used',
            choices=[1, 2], type=int, default=2)

    # sgready config
    parser.add_argument("--hp-heating-rate", type=float, default=HeatpumpTestService.DEFAULT_HP_HEATING_RATE_C_PER_H,
                        help="tank heating rate in C/h while running (default: 12)")
    parser.add_argument("--temp-low", type=float, default=HeatpumpTestService.DEFAULT_TEMP_LOW_C,
                        help="low energy state temperature in C (default: 33)")
    parser.add_argument("--temp-normal-setpoint", type=float, default=HeatpumpTestService.DEFAULT_TEMP_NORMAL_SETPOINT_C,
                        help="normal setpoint in C (default: 60)")
    parser.add_argument("--temp-sgready-setpoint", type=float, default=HeatpumpTestService.DEFAULT_TEMP_SGREADY_SETPOINT_C,
                        help="sgready setpoint in C (default: 75)")
    parser.add_argument("--hysteresis", type=float, default=HeatpumpTestService.DEFAULT_HYSTERESIS_C,
                        help="hysteresis in C (default: 5)")
    parser.add_argument("--min-runtime", type=float, default=HeatpumpTestService.DEFAULT_MIN_RUNTIME_S,
                        help="minimum runtime in seconds (default: 600)")
    parser.add_argument("--min-stopped", type=float, default=HeatpumpTestService.DEFAULT_MIN_STOPPED_S,
                        help="minimum stopped time in seconds (default: 600)")
    parser.add_argument("--leakage-base", type=float, default=HeatpumpTestService.DEFAULT_LEAKAGE_BASE_C_PER_H,
                        help="base leakage in C/h (default: 2.2)")
    parser.add_argument("--leakage-use", type=float, default=HeatpumpTestService.DEFAULT_LEAKAGE_USE_C_PER_H,
                        help="usage leakage in C/h (default: 1.2)")
    parser.add_argument("--leakage-event-interval", type=float, default=HeatpumpTestService.DEFAULT_LEAKAGE_EVENT_INTERVAL_S,
                        help="average event interval in s (default: 2700)")
    parser.add_argument("--leakage-event-jitter", type=float, default=HeatpumpTestService.DEFAULT_LEAKAGE_EVENT_JITTER_S,
                        help="event interval jitter in s (default: 1200)")
    parser.add_argument("--leakage-event-drop-min", type=float, default=HeatpumpTestService.DEFAULT_LEAKAGE_EVENT_DROP_C_MIN,
                        help="minimum random event drop in C (default: 0.2)")
    parser.add_argument("--leakage-event-drop-max", type=float, default=HeatpumpTestService.DEFAULT_LEAKAGE_EVENT_DROP_C_MAX,
                        help="maximum random event drop in C (default: 1.2)")
    parser.add_argument("--debug", action="store_true", help="enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    bus_type = {"system": BusType.SYSTEM, "session": BusType.SESSION}.get(args.dbus, BusType.SYSTEM)
    bus = await MessageBus(bus_type=bus_type).connect()

    relay_index = None if args.relay is None else (args.relay - 1)

    svc = HeatpumpTestService(
        bus,
        mode="interactive" if args.interactive else args.mode,
        interactive=args.interactive,
        nominal_w=args.nominal,
        spikiness=args.spikiness,
        hp_heating_rate_c_per_h=args.hp_heating_rate,
        temp_low_c=args.temp_low,
        temp_normal_setpoint_c=args.temp_normal_setpoint,
        temp_sgready_setpoint_c=args.temp_sgready_setpoint,
        hysteresis_c=args.hysteresis,
        min_runtime_s=args.min_runtime,
        min_stopped_s=args.min_stopped,
        leakage_base_c_per_h=args.leakage_base,
        leakage_use_c_per_h=args.leakage_use,
        leakage_event_interval_s=args.leakage_event_interval,
        leakage_event_jitter_s=args.leakage_event_jitter,
        leakage_event_drop_c_min=args.leakage_event_drop_min,
        leakage_event_drop_c_max=args.leakage_event_drop_max,
    )
    await svc.register()

    if relay_index is not None:
        await RelayMonitor.create(
            bus,
            heatpump_service=svc,
            relay_index=relay_index,
        )
        logger.info("Monitoring GX relay %d for SG Ready control", args.relay)

    logger.info(
        "Service up: %s (mode=%s, nominal=%.0fW, spikiness=%d)",
        svc.SERVICE_NAME, svc._mode, args.nominal, args.spikiness,
    )

    if svc._interactive:
        await svc.interactive_cli()
    else:
        await bus.wait_for_disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass