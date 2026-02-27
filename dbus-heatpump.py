#!/usr/bin/python3

from __future__ import annotations

import os
import sys
import asyncio
import logging
from dataclasses import dataclass
from argparse import ArgumentParser

from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType


# aiovelib
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'ext', 'aiovelib'))

from aiovelib.service import IntegerItem, Service
from aiovelib.client import Monitor, Service as ObservableService


from s2 import S2ResourceManagerItem
from utils import Power, HeatpumpPowerEstimator as PowerEstimator

logger = logging.getLogger(__name__)


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
    paths = [
        "/Relay/0/State",
        "/Relay/1/State",
    ]

    @property
    def relay_1(self):
        return self.get_value("/Relay/0/State")

    @relay_1.setter
    def relay_1(self, val: int):
        return self.set_value_async("/Relay/0/State", val)

    @property
    def relay_2(self):
        return self.get_value("/Relay/1/State")

    @relay_2.setter
    def relay_2(self, val: int):
        return self.set_value_async("/Relay/1/State", val)


class HeatpumpControlService(Service):

    OFF_HYSTERESIS_S: int = 300
    ON_HYSTERESIS_S:  int = 300
    POWER_SETTING_W:  int = 2000
    RUNNING_THRESH_W: int = 200

    def __init__(self, bus, system_service: SystemService, heatpump_service: HeatpumpService):
        super().__init__(bus=bus, name='com.victronenergy.hpcontrol')

        self._system = system_service
        self._heatpump = heatpump_service
        self._estimator: PowerEstimator | None = None

    @property
    def s2_rm(self) -> S2ResourceManagerItem:
        return self.get_item('/S2/0/Rm')

    @property
    def s2_active(self) -> int:
        o = self.get_item('/S2/0/Active')
        return o.value or 0 if o else None

    @s2_active.setter
    def s2_active(self, value: int):
        o =  self.get_item('/S2/0/Active')
        if o: o.set_local_value(value)

    @property
    def on_hysteresis(self) -> int:
        o = self.get_item('/S2/0/RmSettings/OnHysteresis').value
        return o.value or 0 if o else None

    @property
    def off_hysteresis(self) -> int:
        o = self.get_item('/S2/0/RmSettings/OffHysteresis').value
        return o.value or 0 if o else None

    @property
    def power_setting(self) -> int:
        o = self.get_item('/S2/0/RmSettings/PowerSetting')
        return o.value or 0 if o else None

    @property
    def running_threshold(self) -> int:
        o = self.get_item('/S2/0/RmSettings/RunningThreshold')
        return o.value or 0 if o else None

    async def register(self):
        self.add_item(S2ResourceManagerItem('/S2/0/Rm'))
        self.add_item(IntegerItem('/S2/0/Active', 0, text=lambda v: "YES" if v > 0 else "NO"))
        self.add_item(IntegerItem('/S2/0/RmSettings/OffHysteresis', self.OFF_HYSTERESIS_S,
                                  text=lambda v: f"{v:.0f} s"))
        self.add_item(IntegerItem('/S2/0/RmSettings/OnHysteresis', self.ON_HYSTERESIS_S,
                                  text=lambda v: f"{v:.0f} s"))
        self.add_item(IntegerItem('/S2/0/RmSettings/PowerSetting', self.POWER_SETTING_W,
                                  writeable=True, onchange=self._on_nominal_total_change,
                                  text=lambda v: f"{v:.0f} W"))
        self.add_item(IntegerItem('/S2/0/RmSettings/RunningThreshold', self.RUNNING_THRESH_W,
                                  writeable=True, onchange=self._on_running_thresh_change,
                                  text=lambda v: f"{v:.0f} W"))

        self.add_item(IntegerItem('/CurrentPower', None,
                                  text=lambda v: f"{v:.0f} W"))
        self.add_item(IntegerItem('/EstimatedPower', None,
                                  text=lambda v: f"{v:.0f} W"))

        self._estimator = PowerEstimator(
            nominal_total_w=self.power_setting,
            phases=self._heatpump.phases,
            running_threshold_w=self.running_threshold
        )
        self._set_estimated_power()
        await super().register()

    def _set_current_power(self, val: int):
        o = self.get_item('/CurrentPower')
        o.set_local_value(val)

    def _set_estimated_power(self):
        est = self._estimator.estimated_power()
        o = self.get_item('/EstimatedPower')
        o.set_local_value(est.total)

    def _on_nominal_total_change(self, val: int):
        if self._estimator:
            self._estimator.set_nominal_total_w(val)
            logger.info(f"Nominal power changed to {val} W")
            return True
        return False

    def _on_running_thresh_change(self, val: int):
        if self._estimator:
            self._estimator.set_running_threshold_w(val)
            logger.info(f"Running threshold changed to {val} W")
            return True
        return False

    def itemsChanged(self, service: ObservableService, values):
        if not self._estimator:
            return

        if isinstance(service, HeatpumpService):
            if not service.power.valid:
                return

            changed_significantly = self._estimator.feed(service.power)
            if changed_significantly:
                self._set_estimated_power()
            self._set_current_power(service.power.total)

        elif isinstance(service, SystemService):
            logger.info("%s: relay1=%s relay2=%s ", service.name,
                        service.relay_1, service.relay_2)


class HeatpumpMonitor(Monitor):
    def __init__(self, bus, **kwargs):
        super().__init__(bus, handlers={
            HeatpumpService.servicetype: HeatpumpService,
            SystemService.servicetype: SystemService
        }, **kwargs)

        self._heatpumps: set[HeatpumpService] = set()
        self._system: SystemService | None = None
        self._control_service: HeatpumpControlService | None = None

    @property
    def _heatpump_names(self) -> list[str]:
        return sorted([hp.name for hp in list(self._heatpumps)])

    @property
    def _heatpump(self) -> HeatpumpService:
        return list(self._heatpumps)[0]

    async def _check_lifecycle(self):
        if all((
            self._system is not None,
            len(self._heatpumps) == 1
        )): await self._start_control()
        else: await self._stop_control()

    async def _start_control(self):
        if self._control_service is None:
            logger.info("Starting Heatpump Control Service")
            self._control_service = HeatpumpControlService(
                self.bus, self._system, self._heatpump)
            await self._control_service.register()

    async def _stop_control(self):
        if self._control_service is not None:
            logger.info("Stopping Heatpump Control Service")
            await self._control_service.close()
            self._control_service = None

    async def serviceAdded(self, service: ObservableService):
        if isinstance(service, HeatpumpService):
            self._heatpumps.add(service)
        elif isinstance(service, SystemService):
            if not self._system:
                self._system = service
        await self._check_lifecycle()

    async def serviceRemoved(self, service: ObservableService):
        if isinstance(service, HeatpumpService):
            self._heatpumps.discard(service)
        elif isinstance(service, SystemService):
            self._system = None
        await self._check_lifecycle()

    def itemsChanged(self, service: ObservableService, values):
        if self._control_service:
            self._control_service.itemsChanged(service, values)

async def main():

    parser = ArgumentParser(description=sys.argv[0])
    parser.add_argument('--dbus', help='dbus bus to use, defaults to system',
            default='system')
    parser.add_argument('--debug', help='Turn on debug logging',
            default=False, action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    bus_type = {
        "system": BusType.SYSTEM,
        "session": BusType.SESSION
    }.get(args.dbus, BusType.SYSTEM)

    bus = await MessageBus(bus_type=bus_type).connect()
    _ = await HeatpumpMonitor.create(bus)

    try:
        await bus.wait_for_disconnect()
        logger.info("Terminating")
    except asyncio.CancelledError:
        pass
    finally:
        try:
            bus.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass