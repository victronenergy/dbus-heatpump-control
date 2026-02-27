#!/usr/bin/python3


import os
import sys
import asyncio
import logging
from argparse import ArgumentParser

from dbus_fast.aio import MessageBus
from dbus_fast.constants import BusType


# aiovelib
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'ext', 'aiovelib'))

from aiovelib.client import Monitor, Service as ObservableService


logger = logging.getLogger(__name__)


class HeatpumpService(ObservableService):
    servicetype = "com.victronenergy.heatpump"
    paths = [
        "/DeviceInstance",
        "/Ac/L1/Power",
        "/Ac/L2/Power",
        "/Ac/L3/Power",
        "/Ac/Power",
        "/NrOfPhases",
    ]

class SystemService(ObservableService):
    servicetype = "com.victronenergy.system"
    paths = [
        "/Relay/0/State",
        "/Relay/1/State",
    ]


class HeatpumpMonitor(Monitor):
    def __init__(self, bus, **kwargs):
        super().__init__(bus, handlers={
            HeatpumpService.servicetype: HeatpumpService,
            SystemService.servicetype: SystemService
        }, **kwargs)
        self._heatpump_names = set()

    def _check_count(self):
        n = len(self._heatpump_names)
        if n == 1:
            return
        if n == 0:
            logger.info("⚠️  No heatpump service present")
        else:
            logger.info(
                "⚠️  More than one heatpump service present (%d): %s",
                n, ", ".join(sorted(self._heatpump_names))
            )

    async def serviceAdded(self, service: ObservableService):
        if isinstance(service, HeatpumpService):
            self._heatpump_names.add(service.name)
            self._check_count()
        elif isinstance(service, SystemService):
            r1 = service.get_value("/Relay/0/State")
            r2 = service.get_value("/Relay/1/State")
            logger.info("%s: relay1=%s relay2=%s ", service.name, r1, r2)
            # await service.set_value("/Relay/0/State", 1)

    async def serviceRemoved(self, service: ObservableService):
        if isinstance(service, HeatpumpService):
            self._heatpump_names.discard(service.name)
            self._check_count()

    def itemsChanged(self, service: ObservableService, values):
        if not isinstance(service, HeatpumpService):
            return

        l1 = service.get_value("/Ac/L1/Power")
        l2 = service.get_value("/Ac/L2/Power")
        l3 = service.get_value("/Ac/L3/Power")
        ac = service.get_value("/Ac/Power")
        n  = service.get_value("/NrOfPhases")

        logger.info("%s: NrOfPhases=%s Ac/Power=%s L1=%s L2=%s L3=%s",
                    service.name, n, ac, l1, l2, l3)


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
    mon = await HeatpumpMonitor.create(bus)

    mon._check_count()

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
        print("Terminating")