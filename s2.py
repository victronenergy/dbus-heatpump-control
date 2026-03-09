import uuid
import logging
import asyncio
from datetime import datetime, timezone
from typing import Awaitable

from s2python.common import (
    CommodityQuantity,
    Duration,
    PowerRange,
    ReceptionStatusValues,
    PowerMeasurement,
    PowerValue,
    Timer,
    Transition,
)
from s2python.s2_control_type import NoControlControlType, OMBCControlType
from s2python.ombc import OMBCInstruction, OMBCOperationMode, OMBCStatus, OMBCSystemDescription


logger = logging.getLogger(__name__)


class HeatpumpNOCTRL(NoControlControlType):
    def __init__(self, ctrl: "HeatPumpControlService"): # type: ignore
        super().__init__()
        self._ctrl = ctrl
        self._active = False

    @property
    def active(self):
        return self._active

    def activate(self, conn):
        self._active = True
        logger.info("NOCTRL activated")

    def deactivate(self, conn):
        self._active = False
        logger.info("NOCTRL deactivated")


class HeatpumpOMBC(OMBCControlType):
    def __init__(self, ctrl: "HeatPumpControlService"): # type: ignore
        self._ctrl = ctrl
        self._active = False
        self._previous_operation_mode_id: str | None = None

        self._id_off = uuid.uuid4()
        self._id_on = uuid.uuid4()
        self._id_on_off = uuid.uuid4()
        self._id_off_on = uuid.uuid4()

        self._pm_task: asyncio.Task | None = None
        self._apply_task: asyncio.Task | None = None

        super().__init__()

    @property
    def active(self):
        return self._active

    def _make_system_description(self) -> OMBCSystemDescription:
        commodity = phases_to_commodity(self._ctrl.hp_phases)

        est = int(self._ctrl.estimated_power_w)
        op_on = OMBCOperationMode(
            id=str(self._id_on),
            diagnostic_label="On",
            abnormal_condition_only=False,
            power_ranges=[PowerRange(
                start_of_range=est,
                end_of_range=est,
                commodity_quantity=commodity,
            )],
        )

        op_off = OMBCOperationMode(
            id=str(self._id_off),
            diagnostic_label="Off",
            abnormal_condition_only=False,
            power_ranges=[PowerRange(
                start_of_range=0,
                end_of_range=0,
                commodity_quantity=commodity,
            )],
        )

        on_timer = Timer(
            id=uuid.uuid4(),
            diagnostic_label="On hysteresis",
            duration=Duration.from_milliseconds(self._ctrl.items.on_hysteresis * 1000),

        )
        off_timer = Timer(
            id=uuid.uuid4(),
            diagnostic_label="Off hysteresis",
            duration=Duration.from_milliseconds(self._ctrl.items.off_hysteresis * 1000),
        )

        t_to_on = Transition(
            id=str(self._id_off_on),
            from_=op_off.id,
            to=op_on.id,
            start_timers=[off_timer.id],
            blocking_timers=[on_timer.id],
            transition_duration=None,
            abnormal_condition_only=False,
        )
        t_to_off = Transition(
            id=str(self._id_on_off),
            from_=op_on.id,
            to=op_off.id,
            start_timers=[on_timer.id],
            blocking_timers=[off_timer.id],
            transition_duration=None,
            abnormal_condition_only=False,
        )

        return OMBCSystemDescription(
            message_id=uuid.uuid4(),
            valid_from=datetime.now(timezone.utc),
            operation_modes=[op_on, op_off],
            transitions=[t_to_on, t_to_off],
            timers=[on_timer, off_timer],
        )

    async def handle_instruction(self, conn, msg, send_okay: Awaitable[None]):
        if not isinstance(msg, OMBCInstruction):
            return
        if not self._active:
            return

        if str(msg.operation_mode_id) not in (str(self._id_off), str(self._id_on)):
            logger.warning("Unknown operation mode id: %s", msg.operation_mode_id)
            return

        # cancel a previous task if, if still not done
        prev = self._apply_task
        if prev is not None:
            if not prev.done():
                prev.cancel()
            try:
                await prev
            except asyncio.CancelledError:
                pass

        # respect execution_time if present
        delay = 0.0
        if msg.execution_time is not None:
            delay = max(0.0, (msg.execution_time - datetime.now(timezone.utc)).total_seconds())

        self._apply_task = asyncio.create_task(self._apply_mode(msg.operation_mode_id, delay))
        await send_okay

    async def _apply_mode(self, op_mode_id: str, delay: float):
        try:
            if delay:
                await asyncio.sleep(delay)

            on = (str(op_mode_id) == str(self._id_on))
            await self._ctrl._set_relay_on(on)
        except asyncio.CancelledError:
            logger.warning("Apply mode task for mode id %s got cancelled", op_mode_id)
            pass

    async def activate(self, conn):
        self._active = True
        self._ctrl.items.s2_active = 1

        # send system description; if not OK, roll back
        resp = await self.send_system_description()
        if (resp is None) or (resp.status != ReceptionStatusValues.OK):
            logger.error("OMBC activation failed, reception: %s", resp)
            self.deactivate(conn)
            return

        # send initial status + power
        await self.send_status(self._ctrl.state_on)
        await self.send_power_measurement()
        logger.info("OMBC activated")

    async def deactivate(self, conn):
        self._active = False
        self._ctrl.items.s2_active = 0
        await self._ctrl._set_relay_on(False)
        if self._pm_task and not self._pm_task.done():
            self._pm_task.cancel()
        logger.info("OMBC deactivated")

    async def send_system_description(self):
        if not self._active:
            return
        try:
            desc = self._make_system_description()
            await self._ctrl.rm_item.send_msg_and_await_reception_status(desc)
            return await self.send_status(self._ctrl.state_on)
        except Exception as e:
            logger.error("Failed to send OMBCSystemDescription: %s", e)

    async def send_status(self, on: bool):
        if not self._active:
            return

        op_id = str(self._id_on if on else self._id_off)

        try:
            return await self._ctrl.rm_item.send_msg_and_await_reception_status(
                OMBCStatus(
                    message_id=uuid.uuid4(),
                    active_operation_mode_id=op_id,
                    operation_mode_factor=1,
                    previous_operation_mode_id=self._previous_operation_mode_id,
                    transition_timestamp=datetime.now(timezone.utc),
                )
            )
        except Exception as e:
            logger.error("Failed to send OMBCStatus: %s", e)
        finally:
            self._previous_operation_mode_id = op_id

    def schedule_power_measurement(self):
        if self._pm_task is None or self._pm_task.done():
            self._pm_task = asyncio.create_task(self.send_power_measurement())

    async def send_power_measurement(self):
        if not self._active:
            return

        commodity = phases_to_commodity(self._ctrl.hp_phases)

        # measured total
        measured = int(self._ctrl.items.current_power or 0)

        try:
            await self._ctrl.rm_item.send_msg_and_await_reception_status(
                PowerMeasurement(
                    message_id=uuid.uuid4(),
                    measurement_timestamp=datetime.now(timezone.utc),
                    values=[
                        PowerValue(commodity_quantity=commodity, value=measured),
                    ],
                )
            )
        except Exception as e:
            logger.error("Failed to send PowerMeasurement: %s", e)


def phases_to_commodity(phases: int | None) -> CommodityQuantity:
    if phases == 3:
        return CommodityQuantity.ELECTRIC_POWER_3_PHASE_SYMMETRIC
    return CommodityQuantity.ELECTRIC_POWER_L1


class S2Adapter:
    """
    Wraps:
      - OMBC/noctrl activation state
      - sysdesc coalescing (only when OMBC active)
      - power measurement scheduling (even when NoCtrl active)
      - status updates (OMBC only)
    """
    def __init__(self, *, ctrl, rm_item, ombc, noctrl):
        self.ctrl = ctrl
        self.rm_item = rm_item
        self.ombc = ombc
        self.noctrl = noctrl

        self._sysdesc_task: asyncio.Task | None = None
        self._sysdesc_pending = False

        self._pm_task: asyncio.Task | None = None

    @property
    def ombc_active(self) -> bool:
        return bool(getattr(self.ombc, "active", False))

    @property
    def noctrl_active(self) -> bool:
        return bool(getattr(self.noctrl, "active", False))

    @property
    def any_active(self) -> bool:
        return self.ombc_active or self.noctrl_active

    # ---- sysdesc ----

    def request_system_description(self) -> None:
        if not self.ombc_active:
            return

        if self._sysdesc_task and not self._sysdesc_task.done():
            self._sysdesc_pending = True
            return

        async def _send():
            try:
                await self.ombc.send_system_description()
            finally:
                if self._sysdesc_pending:
                    self._sysdesc_pending = False
                    await asyncio.sleep(0.5)
                    await self.ombc.send_system_description()

        self._sysdesc_task = asyncio.create_task(_send())

    # ---- power measurement (works for OMBC and NOCTRL) ----

    def schedule_power_measurement(self) -> None:
        if not self.any_active:
            return
        if self._pm_task is None or self._pm_task.done():
            self._pm_task = asyncio.create_task(self._send_power_measurement())

    async def _send_power_measurement(self) -> None:
        commodity = phases_to_commodity(self.ctrl.hp_phases)
        measured = int(self.ctrl.items.current_power or 0)
        pm = PowerMeasurement(
            message_id=uuid.uuid4(),
            measurement_timestamp=datetime.now(timezone.utc),
            values=[PowerValue(commodity_quantity=commodity, value=measured)],
        )
        try:
            await self.rm_item.send_msg_and_await_reception_status(pm)
        except Exception as e:
            logger.error("Failed to send PowerMeasurement: %s", e)

    # ---- status ----

    def notify_state_changed(self, on: bool) -> None:
        if self.ombc_active:
            asyncio.create_task(self.ombc.send_status(on))
