#!/usr/bin/python3
"""
com.victronenergy.heatpump.mock

Default:
  Automatic cycle:
    - 12 min ON / 6 min OFF
    - Smooth ramp + configurable noise ("spikiness")

With --interactive / -i:
  Manual CLI control:
    - on / off
    - set <watts>
    - spike <0-100>
    - status
    - help
    - quit
"""

from __future__ import annotations

import os
import sys
import math
import time
import asyncio
import logging
from argparse import ArgumentParser

from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "ext", "aiovelib"))
from aiovelib.service import Service, IntegerItem, DoubleItem  # type: ignore

logger = logging.getLogger(__name__)


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


class HeatpumpTestService(Service):
    SERVICE_NAME = "com.victronenergy.heatpump.mock"

    V_AC = 230.0
    PF_ON = 0.95
    PF_OFF = 0.0

    ON_S = 12 * 60
    OFF_S = 6 * 60
    PERIOD_S = ON_S + OFF_S

    DEFAULT_NOMINAL_W = 2000.0
    DEFAULT_SPIKINESS = 4          # 0..100
    UPDATE_DT = 1.0

    def __init__(
        self,
        bus,
        *,
        interactive: bool = False,
        nominal_w: float = DEFAULT_NOMINAL_W,
        spikiness: int = DEFAULT_SPIKINESS,
    ):
        super().__init__(bus=bus, name=self.SERVICE_NAME)

        self._interactive = bool(interactive)

        self._t0 = time.monotonic()
        self._last = self._t0

        self._l1_power_w = 0.0
        self._e_fwd_kwh_total = 0.0
        self._e_fwd_kwh_l1 = 0.0
        self._e_rev_kwh_total = 0.0
        self._e_rev_kwh_l1 = 0.0

        # interactive controls
        self._forced_on = False
        self._nominal_w = float(nominal_w)
        self._spikiness = int(_clamp(float(spikiness), 0.0, 100.0))

        self._lock = asyncio.Lock()

    # ------------------------------------------------------------
    # Public control API used by CLI
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

    async def get_status(self) -> dict:
        async with self._lock:
            return {
                "mode": "interactive" if self._interactive else "auto",
                "forced_on": self._forced_on,
                "nominal_w": self._nominal_w,
                "spikiness": self._spikiness,
                "power_w": float(self._l1_power_w),
                "energy_fwd_kwh": float(self._e_fwd_kwh_total),
            }

    # ------------------------------------------------------------

    def _is_on_auto(self, t: float) -> bool:
        ph = t % self.PERIOD_S
        return ph < self.ON_S

    def _noise(self, amp: float) -> float:
        # uniform-ish noise in [-amp, +amp]
        return (2.0 * (os.urandom(1)[0] / 255.0) - 1.0) * amp

    def _noise_amp(self, nominal_w: float, spikiness: int) -> float:
        """
        spikiness: 0..10
          0  -> 0% of nominal
          10 -> 10% of nominal
        """
        frac = (float(spikiness) / 10.0) * 0.10
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

        # Misc
        self.add_item(IntegerItem("/NrOfPhases", 1))
        self.add_item(IntegerItem("/DeviceType", 0))
        self.add_item(IntegerItem("/ErrorCode", 0))
        self.add_item(IntegerItem("/IsGenericEnergyMeter", 0))
        self.add_item(IntegerItem("/DeviceInstance", 200))

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
                if self._interactive:
                    on = self._forced_on
                else:
                    on = self._is_on_auto(t)
                nominal = self._nominal_w
                spk = self._spikiness

            noise_amp = self._noise_amp(nominal, spk)

            if on:
                tgt = nominal + self._noise(noise_amp)
            else:
                # tiny standby noise (independent from spikiness)
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

            await asyncio.sleep(self.UPDATE_DT)

    # ------------------------------------------------------------
    # CLI
    # ------------------------------------------------------------

    async def interactive_cli(self):
        print("Interactive mode.")
        print("Commands: on | off | set <watts> | spike <0-100> | status | help | quit")

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
                    print(f"Spikiness set to {s} (noise ≈ {(s/10)*10:.1f}% of nominal)")

                elif cmd == "status":
                    st = await self.get_status()
                    print(
                        f"Mode={st['mode']} ForcedOn={st['forced_on']} "
                        f"Nominal={st['nominal_w']:.0f}W Spikiness={st['spikiness']} "
                        f"Power={st['power_w']:.0f}W EnergyFwd={st['energy_fwd_kwh']:.3f}kWh"
                    )

                elif cmd in ("help", "?"):
                    print("Commands:")
                    print("  on             - force heatpump on")
                    print("  off            - force heatpump off")
                    print("  set <watts>    - set nominal power (W)")
                    print("  spike <0-100>  - set spikiness (noise amplitude)")
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
    parser.add_argument("--nominal", type=float, default=HeatpumpTestService.DEFAULT_NOMINAL_W,
                        help="nominal power in W (default: 2000)")
    parser.add_argument("--spikiness", type=int, default=HeatpumpTestService.DEFAULT_SPIKINESS,
                        help="0..10, noise amplitude up to ~10%% of nominal (default: 4)")
    parser.add_argument("--debug", action="store_true", help="enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    bus_type = {"system": BusType.SYSTEM, "session": BusType.SESSION}.get(args.dbus, BusType.SYSTEM)
    bus = await MessageBus(bus_type=bus_type).connect()

    svc = HeatpumpTestService(
        bus,
        interactive=args.interactive,
        nominal_w=args.nominal,
        spikiness=args.spikiness,
    )
    await svc.register()

    logger.info(
        "Service up: %s (nominal=%.0fW, spikiness=%d, mode=%s)",
        svc.SERVICE_NAME, args.nominal, args.spikiness,
        "interactive" if args.interactive else "auto",
    )

    if args.interactive:
        await svc.interactive_cli()
    else:
        await bus.wait_for_disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass