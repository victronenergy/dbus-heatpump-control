#!/usr/bin/python3

from __future__ import annotations

import os
import sys
import time
import math
import uuid
import asyncio
import logging
from argparse import ArgumentParser

from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType

from s2python.s2_asset_details import AssetDetails
from s2python.generated.gen_s2 import RoleType
from s2python.common import Role, Duration, Commodity

# aiovelib
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'ext', 'aiovelib'))

from aiovelib.service import IntegerItem, Service, TextItem
from aiovelib.client import Monitor, Service as ObservableService

from version import VERSION
from s2 import (
    S2Adapter,
    S2ResourceManagerItem,
    HeatpumpOMBC,
    HeatpumpNOCTRL,
    phases_to_commodity,
)
from utils import (
    SERVICE_STATE,
    EnumItem,
    EstimatorManager,
    RelayChannel,
    SettingsService,
    SystemService,
    HeatpumpService,
    RELAY_STATE,
    RelayConfig,
    Relays,
    HpItems,
    HeatpumpPowerEstimator as PowerEstimator,
)


logger = logging.getLogger(__name__)


SERVICE_NAME = "com.victronenergy.heatpumpcontrol"

class HeatPumpControlService(Service):

    productname = "Heat pump control"

    OFF_HYSTERESIS_S: int = 600
    ON_HYSTERESIS_S: int = 600
    POWER_SETTING_W: int = 2000
    RUNNING_THRESH_W: int = 200

    DEFAULT_RELAY_INDEX: int = 1  # default, 0-based
    REQUIRED_RELAY_FUNCTION: int = 6 # Opportunity Loads

    MAX_EST_UPDATE_S: int = 30  # wait at least 30s between power estimate updates

    def __init__(self, bus, relay_index: int | None,
                 system_service: SystemService,
                 settings_service: SettingsService,
                 heatpump_service: HeatpumpService):
        super().__init__(bus=bus, name=SERVICE_NAME)

        self._relay_index = relay_index if relay_index is not None else self.DEFAULT_RELAY_INDEX

        self._system: SystemService = system_service
        self._settings: SettingsService = settings_service
        self._heatpump: HeatpumpService = heatpump_service

        self.items = HpItems(self, self._settings)
        self.relays = Relays(self._system, self._settings, count=2,
                             cfg=RelayConfig(required_function=self.REQUIRED_RELAY_FUNCTION))

        self.est_mgr = EstimatorManager(PowerEstimator)

        self._rm_item = None
        self._ombc = None
        self._noctrl = None
        self.s2: S2Adapter | None = None

        self._prev_relay_state_change: float | None = None
        self._last_estimate_update: float | None = time.monotonic()

    # ---- small domain properties used by OMBC / adapter ----

    @property
    def rm_item(self):
        return self._rm_item

    @property
    def hp_phases(self) -> int | None:
        # prefer estimator’s current (after phase change logic)
        return self.est_mgr.hp_phases if self.est_mgr.hp_phases is not None else self._heatpump.phases

    @property
    def estimated_power_w(self) -> int:
        # always comes from the DBus item to stay consistent with what we publish
        return int(self.items.estimated_power)

    @property
    def state_on(self) -> bool:
        return bool(self.items.state == SERVICE_STATE.ON)

    @property
    def relay(self) -> RelayChannel:
        return self.relays[self._relay_index]

    # ---- relay control (logical state) ----

    async def _set_relay_on(self, on: bool) -> None:
        try:

            # Warn if command violates hystereses
            prev_on: bool = self.relay.state == RELAY_STATE.ON
            if prev_on != on:
                logger.debug(f"Switching relay to { 'ON' if on else 'OFF' }")
                prev_ts = self._prev_relay_state_change
                if prev_ts:
                    diff = time.monotonic() - prev_ts
                    if on and diff < self.items.on_hysteresis:
                        logger.warning(f"Switching ON, but was switched off only { int(diff) } s ago")
                    elif not on and diff < self.items.off_hysteresis:
                        logger.warning(f"Switching OFF, but was switched on only { int(diff) } s ago")
                self._prev_relay_state_change = time.monotonic()

            await self.relay.set_state(RELAY_STATE.ON if on else RELAY_STATE.OFF)
        except Exception as e:
            logger.exception("Relay control failed: %s", e)
            await self._publish_allowed_control_types()
        finally:
            self._refresh_relay_state_from_services()

    def _refresh_relay_state_from_services(self) -> None:
        st = self.relay.state
        if st is None:
            return
        self.items.state = SERVICE_STATE.ON if st == RELAY_STATE.ON else SERVICE_STATE.OFF

    def _relay_function_ok(self) -> bool:
        try:
            return self.relay.controllable
        except Exception:
            return False

    def _is_ombc_allowed(self):
        return self._relay_function_ok()

    async def _publish_allowed_control_types(self) -> None:
        """
        Update what we offer to CEM:
          - Function ok: [NOCTRL, OMBC]
          - Function not ok: [NOCTRL]
        Also force OMBC inactive if it was active but is no longer allowed.
        """
        if not all((
            self.rm_item,
            self.rm_item.is_ready,
            self.rm_item.is_connected
        )):
            return

        allow_ombc = self._is_ombc_allowed()

        # If OMBC is active but no longer allowed -> force it off locally.
        if not allow_ombc and getattr(self, "_ombc", None) and self._ombc.active:
            try:
                # Prefer the control type to deactivate itself
                self._ombc.deactivate(None)
            except Exception:
                # fallback: at least reflect locally
                self.items.s2_active = 0

        control_types = [self._noctrl] + ([self._ombc] if allow_ombc else [])

        # Tell CEM "these are the only allowed control types right now"
        try:
            await self.rm_item.send_resource_manager_details(
                control_types=control_types,
                asset_details=self.rm_item.asset_details,  # reuse existing
            )
        except Exception as e:
            logger.warning("Failed to publish allowed control types: %s", e)

    def round_up_to_50(self, x: float) -> int:
        return int(math.ceil(x / 50.0) * 50)

    # ---- register ----

    async def register(self):
        if self._relay_function_ok():
            await self._set_relay_on(False)

        # Create settings with defaults, if missing
        await self._settings.add_rm_settings(
            self.ON_HYSTERESIS_S, self.OFF_HYSTERESIS_S, self.POWER_SETTING_W, self.RUNNING_THRESH_W)

        # S2 RM
        details = AssetDetails(
            resource_id=uuid.uuid4(),
            provides_forecast=False,
            provides_power_measurements=[phases_to_commodity(self._heatpump.phases)],
            instruction_processing_delay=Duration.from_milliseconds(0),
            roles=[Role(role=RoleType.ENERGY_CONSUMER, commodity=Commodity.ELECTRICITY)],
            name=self.productname,
            manufacturer="Victron Energy",
            firmware_version="1",
            serial_number=str(self._heatpump.get_value("/DeviceInstance") or "0"),
        )

        self._noctrl = HeatpumpNOCTRL(self)
        self._ombc = HeatpumpOMBC(self)

        self._rm_item = S2ResourceManagerItem(
            "/S2/0/Rm",
            control_types=(
                [self._noctrl] + ([self._ombc] if self._is_ombc_allowed() else [])
            ),
            asset_details=details
        )
        self.add_item(self._rm_item)

        # UI items
        self.add_item(IntegerItem("/S2/0/Active", 0, text=lambda v: "YES" if v > 0 else "NO"))
        self.add_item(IntegerItem("/S2/0/RmSettings/OffHysteresis", self.items.off_hysteresis,
                                  writeable=True, onchange=self._on_off_hysteresis_change,
                                  text=lambda v: f"{v:.0f} s"))
        self.add_item(IntegerItem("/S2/0/RmSettings/OnHysteresis", self.items.on_hysteresis,
                                  writeable=True, onchange=self._on_on_hysteresis_change,
                                  text=lambda v: f"{v:.0f} s"))
        self.add_item(IntegerItem("/S2/0/RmSettings/PowerSetting", self.items.power_setting,
                                  writeable=True, onchange=self._on_power_setting_change,
                                  text=lambda v: f"{v:.0f} W"))
        self.add_item(IntegerItem("/S2/0/RmSettings/RunningThreshold", self.items.running_threshold,
                                  writeable=True, onchange=self._on_running_thresh_change,
                                  text=lambda v: f"{v:.0f} W"))

        self.add_item(IntegerItem("/Relay", self._relay_index,
                                  text=lambda v: f"Relay {v+1}"))

        self.add_item(EnumItem("/State", SERVICE_STATE, value=SERVICE_STATE(self.items.state)))

        self.add_item(TextItem("/Service", self._heatpump.name))
        self.add_item(IntegerItem("/Ac/Power", None, text=lambda v: f"{v:.0f} W" if v is not None else "--"))
        self.add_item(IntegerItem("/EstimatedPower", None, text=lambda v: f"{v:.0f} W" if v is not None else "--"))

        self.add_item(IntegerItem("/DeviceInstance", 0))
        self.add_item(TextItem("/ProductName", self.productname))

        # init estimator
        phases = self._heatpump.phases
        phases = int(phases) if phases in (1, 3) else None
        self.est_mgr.init(nominal_w=self.items.estimated_power, phases=phases, running_thr=self.items.running_threshold)

        # init current + estimated
        self.items.current_power = self._heatpump.power.total
        self.items.estimated_power = self.est_mgr.estimated_total()

        # adapter
        self.s2 = S2Adapter(ctrl=self, rm_item=self._rm_item, ombc=self._ombc, noctrl=self._noctrl)

        # bring things up to date before registering
        self._refresh_relay_state_from_services()
        has_obmc = self._ombc in self.rm_item.control_types
        if has_obmc != self._is_ombc_allowed():
            await self._publish_allowed_control_types()

        await super().register()

        try:
            await self._rm_item.set_ready(True)
        except Exception:
            pass

    # ---- onchange callbacks ----

    def _on_off_hysteresis_change(self, val: int):
        self.items.off_hysteresis = val
        if self.s2:
            self.s2.request_system_description()
        logger.info("Off hysteresis changed to %s s", val)
        return True

    def _on_on_hysteresis_change(self, val: int):
        self.items.on_hysteresis = val
        if self.s2:
            self.s2.request_system_description()
        logger.info("On hysteresis changed to %s s", val)
        return True

    def _on_power_setting_change(self, val: int):
        self.items.power_setting = val
        self.est_mgr.set_nominal(int(val), mode="auto", clear_history=True)
        self.items.estimated_power = self.est_mgr.estimated_total()
        if self.s2:
            self.s2.request_system_description()
        logger.info("Power setting changed to %s W, estimator got reset", val)
        return True

    def _on_running_thresh_change(self, val: int):
        self.items.running_threshold = val
        self.est_mgr.set_running_threshold(int(val), clear_history=True)
        logger.info("Running threshold changed to %s W", val)
        return True

    # ---- itemsChanged routing ----

    def itemsChanged(self, service: ObservableService, values):
        if not self.s2:
            return

        if isinstance(service, HeatpumpService):
            self._on_heatpump_changed(service, values)
        elif isinstance(service, SystemService):
            self._on_system_changed(service, values)
        elif isinstance(service, SettingsService):
            self._on_settings_changed(service, values)

    def _on_heatpump_changed(self, service: HeatpumpService, values: dict):
        update_sysdesc = False

        # power update
        if service.power.valid:
            self.items.current_power = service.power.total

            if self.state_on:
                # learn only when state is ON
                changed_significantly = self.est_mgr.feed(service.power)

                now = time.monotonic()
                diff = now - self._last_estimate_update
                if changed_significantly and diff >= self.MAX_EST_UPDATE_S:
                    est = self.est_mgr.estimated_total()
                    est_rounded = self.round_up_to_50(est)
                    self.items.estimated_power = est_rounded
                    logging.info(f"Updated estimated power to {est_rounded} W")
                    update_sysdesc = True
                    self._last_estimate_update = now

            # report power measurements when any control type active
            if self.s2.any_active:
                self.s2.schedule_power_measurement()

        # phase change
        if "/NrOfPhases" in values:
            p = service.phases
            if p in (1, 3) and self.est_mgr.set_phases(int(p), keep_expected=True):
                self.items.estimated_power = self.est_mgr.estimated_total()
                update_sysdesc = True

        if update_sysdesc:
            self.s2.request_system_description()

    def _on_system_changed(self, service: SystemService, values: dict):
        # reflect state changes
        st_path = self.relay.state_path()

        if st_path in values:
            self._refresh_relay_state_from_services()

        # status updates only relevant for OMBC
        if self.s2.ombc_active:
            self.s2.notify_state_changed(self.state_on)

    def _on_settings_changed(self, service: SettingsService, values: dict):
        # if polarity changed for our relay, update displayed state immediately

        fn_path = self.relay.function_path()
        pol_path = self.relay.polarity_path()

        if fn_path in values or pol_path in values:
            self._refresh_relay_state_from_services()

        has_obmc = self._ombc in self.rm_item.control_types
        if has_obmc != self._is_ombc_allowed():
            asyncio.create_task(self._publish_allowed_control_types())


class HeatpumpMonitor(Monitor):
    def __init__(self, bus, relay_index, **kwargs):
        super().__init__(bus, handlers={
            HeatpumpService.servicetype: HeatpumpService,
            SystemService.servicetype: SystemService,
            SettingsService.servicetype: SettingsService,
        }, **kwargs)

        self._relay_index: int | None = relay_index
        self._system: SystemService | None = None
        self._settings: SettingsService | None = None
        self._heatpumps: dict[str, HeatpumpService] = {}

        self._control_service: HeatPumpControlService | None = None

    @property
    def _heatpump_names(self) -> list[str]:
        return sorted(self._heatpumps.keys())

    @property
    def _heatpump(self) -> HeatpumpService:
        return next(iter(self._heatpumps.values()))

    async def _check_lifecycle(self):
        if self._system is None:
            logger.info("Waiting for system service ...")
        elif self._settings is None:
            logger.info("Waiting for settings service ...")
        elif len(self._heatpumps) == 0:
            logger.info("Waiting for heatpump service ...")
        elif len(self._heatpumps) > 1:
            logger.warning("More than one heatpump service present: %s", ", ".join(self._heatpump_names))

        if all((
            self._settings,
            self._system,
            len(self._heatpumps) == 1
        )): await self._start_control()
        else: await self._stop_control()

    async def _start_control(self):
        if self._control_service is None:
            self._control_service = HeatPumpControlService(
                self.bus, self._relay_index, self._system, self._settings, self._heatpump)
            logger.info("Ready, starting " + self._control_service.productname or "...")
            await self._control_service.register()

    async def _stop_control(self):
        if self._control_service is not None:
            logger.info("No longer ready, stopping control service")
            await self._control_service.close()
            self._control_service = None

    async def serviceAdded(self, service: ObservableService):
        if isinstance(service, SystemService):
            if not self._system:
                self._system = service
        elif isinstance(service, SettingsService):
            if not self._settings:
                self._settings = service
        elif isinstance(service, HeatpumpService):
            self._heatpumps[service.name] = service
        await self._check_lifecycle()

    async def serviceRemoved(self, service: ObservableService):
        if isinstance(service, HeatpumpService):
            self._heatpumps.pop(service.name, None)
        elif isinstance(service, SettingsService):
            self._settings = None
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
    parser.add_argument('--relay', help='Define GX relay to be used',
            choices=[1, 2], type=int, default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    logger.info(f"*** dbus-heatpump-control {VERSION} ***")

    bus_type = {
        "system": BusType.SYSTEM,
        "session": BusType.SESSION
    }.get(args.dbus, BusType.SYSTEM)

    relay_index = None
    if args.relay is not None:
        relay_index = args.relay-1 # to 0-based

    bus = await MessageBus(bus_type=bus_type).connect()
    _ = await HeatpumpMonitor.create(bus, relay_index=relay_index)

    try:
        await bus.wait_for_disconnect()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"A fatal error occured: {e}")
    finally:
        logger.info("Terminating")
        try:
            bus.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
