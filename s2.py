import json
import uuid
import logging
import asyncio
from datetime import datetime, timezone
from typing import List, Awaitable, Dict

from aiovelib.s2 import S2ServerItem

from s2python.common import (
    CommodityQuantity,
    Duration,
    PowerRange,
    ReceptionStatusValues,
    ReceptionStatus,
    Handshake,
    EnergyManagementRole,
    HandshakeResponse,
    SelectControlType,
    PowerMeasurement,
    PowerValue,
    Timer,
    Transition,
)
from s2python.s2_control_type import S2ControlType, NoControlControlType, OMBCControlType
from s2python.s2_parser import S2Parser
from s2python.s2_validation_error import S2ValidationError
from s2python.message import S2Message
from s2python.version import S2_VERSION
from s2python.s2_asset_details import AssetDetails
from s2python.s2_message_handlers import MessageHandlers

from s2python.ombc import OMBCInstruction, OMBCOperationMode, OMBCStatus, OMBCSystemDescription


logger = logging.getLogger(__name__)


class ReceptionStatusAwaiter:
    """"
    This fixes races and memory leaks compared to the original
    ReceptionStatusAwaiter provided in s2python v0.8.1.
    """
    received: Dict[uuid.UUID, ReceptionStatus]
    awaiting: Dict[uuid.UUID, asyncio.Event]

    def __init__(self) -> None:
        self.received = {}
        self.awaiting = {}

    async def wait_for_reception_status(
        self, message_id: uuid.UUID, timeout_reception_status: float
    ) -> ReceptionStatus:

        existing = self.received.pop(message_id, None)
        if existing is not None:
            return existing

        received_event = self.awaiting.get(message_id)
        if received_event is None:
            received_event = asyncio.Event()
            self.awaiting[message_id] = received_event

        try:
            await asyncio.wait_for(received_event.wait(), timeout_reception_status)
            return self.received.pop(message_id)
        finally:
            self.awaiting.pop(message_id, None)

    async def receive_reception_status(self, reception_status: ReceptionStatus) -> None:
        if not isinstance(reception_status, ReceptionStatus):
            raise RuntimeError(
                f"Expected a ReceptionStatus but received message {reception_status}"
            )

        mid = reception_status.subject_message_id

        if mid in self.received:
            raise RuntimeError(
                f"ReceptionStatus for message_subject_id {mid} has already been received!"
            )

        self.received[mid] = reception_status

        awaiting = self.awaiting.get(mid)
        if awaiting is not None:
            awaiting.set()
            self.awaiting.pop(mid, None)


class S2ResourceManagerItem(S2ServerItem):

    _received_messages: asyncio.Queue
    _restart_connection_event: asyncio.Event

    reception_status_awaiter: ReceptionStatusAwaiter
    s2_parser: S2Parser
    control_types: List[S2ControlType]
    role: EnergyManagementRole
    asset_details: AssetDetails

    _handlers: MessageHandlers
    _current_control_type: S2ControlType | None

    def __init__(self, path,
                 control_types: List[S2ControlType] | None = None,
                 asset_details: AssetDetails | None = None):
        super().__init__(path)

        self.role = EnergyManagementRole.RM
        self.reception_status_awaiter = ReceptionStatusAwaiter()
        self.s2_parser = S2Parser()
        self._handlers = MessageHandlers()
        self._current_control_type = None

        self.control_types = control_types
        self.asset_details = asset_details

        self._main_task: asyncio.Task | None = None

        self._handlers.register_handler(SelectControlType, self.handle_select_control_type_as_rm)
        self._handlers.register_handler(Handshake, self.handle_handshake)
        self._handlers.register_handler(HandshakeResponse, self.handle_handshake_response_as_rm)

    async def set_ready(self, ready: bool, control_types: List[S2ControlType] | None = None, asset_details: AssetDetails | None = None):
        if control_types is not None:
            self.control_types = control_types
        if asset_details is not None:
            self.asset_details = asset_details
        await super().set_ready(ready)

    async def close(self) -> None:
        # Set not ready and destroy existing connection
        await self.set_ready(False)

        # Try to unwind the internal logic to unwind if possible,
        # guarded in case it does not exist yet
        if hasattr(self, "_restart_connection_event"):
            self._restart_connection_event.set()
        if hasattr(self, "_s2_dbus_disconnect_event"):
            self._s2_dbus_disconnect_event.set()

        # Cancel the main task with its children
        task = self._main_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Error while closing S2ResourceManagerItem")

        self._main_task = None

    async def _create_connection(self, client_id: str, keep_alive_interval: int):
        await super()._create_connection(client_id, keep_alive_interval)
        self._main_task = self._runningloop.create_task(self._connect_and_run())

    async def _connect_and_run(self) -> None:
        self._received_messages = asyncio.Queue()
        self._restart_connection_event = asyncio.Event()

        logger.debug("Connecting as S2 resource manager.")

        async def wait_till_disconnect() -> None:
            await self._s2_dbus_disconnect_event.wait()

        async def wait_till_connection_restart() -> None:
            await self._restart_connection_event.wait()

        background_tasks = [
            self._runningloop.create_task(wait_till_disconnect()),
            self._runningloop.create_task(self._connect_as_rm()),
            self._runningloop.create_task(wait_till_connection_restart()),
        ]

        (done, pending) = await asyncio.wait(
            background_tasks, return_when=asyncio.FIRST_COMPLETED
        )
        if self._current_control_type:
            if asyncio.iscoroutinefunction(self._current_control_type.deactivate):
                await self._current_control_type.deactivate(self)
            else:
                await self._runningloop.run_in_executor(
                    None, self._current_control_type.deactivate, self
                )
            self._current_control_type = None

        for task in done:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.exception(f"S2: {e}")

        for task in pending:
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass

        await self._destroy_connection("service shutdown")
        logger.debug("Finished S2 connection eventloop.")

    async def _on_s2_message(self, message):
        try:
            s2_msg: S2Message = self.s2_parser.parse_as_any_message(message)
        except json.JSONDecodeError:
            await self._send_and_forget(
                ReceptionStatus(
                    subject_message_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                    status=ReceptionStatusValues.INVALID_DATA,
                    diagnostic_label="Not valid json.",
                )
            )
        except S2ValidationError as e:
            json_msg = json.loads(message)
            message_id = json_msg.get("message_id")
            if message_id:
                await self._respond_with_reception_status(
                    subject_message_id=message_id,
                    status=ReceptionStatusValues.INVALID_MESSAGE,
                    diagnostic_label=str(e),
                )
            else:
                await self._respond_with_reception_status(
                    subject_message_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                    status=ReceptionStatusValues.INVALID_DATA,
                    diagnostic_label="Message appears valid json but could not find a message_id field.",
                )
        else:
            logger.debug("Received message %s", s2_msg.to_json())

            if isinstance(s2_msg, ReceptionStatus):
                logger.debug(
                    "Message is a reception status for %s so registering in cache.",
                    s2_msg.subject_message_id,
                )
                await self.reception_status_awaiter.receive_reception_status(s2_msg)
            else:
                await self._received_messages.put(s2_msg)

    async def _connect_as_rm(self) -> None:
        await self.send_msg_and_await_reception_status(
            Handshake(
                message_id=uuid.uuid4(),
                role=self.role,
                supported_protocol_versions=[S2_VERSION],
            )
        )
        logger.debug(
            "Send handshake to CEM, expecting Handshake and HandshakeResponse."
        )

        await self._handle_received_messages()

    async def handle_handshake(
        self, _: "S2ResourceManagerItem", message: S2Message, send_okay: Awaitable[None]
    ) -> None:
        if not isinstance(message, Handshake):
            logger.error(
                "Handler for Handshake received a message of the wrong type: %s",
                type(message),
            )
            return

        logger.debug(
            "%s supports S2 protocol versions: %s",
            message.role,
            message.supported_protocol_versions,
        )
        await send_okay

    async def handle_handshake_response_as_rm(
        self, _: "S2ResourceManagerItem", message: S2Message, send_okay: Awaitable[None]
    ) -> None:
        if not isinstance(message, HandshakeResponse):
            logger.error(
                "Handler for HandshakeResponse received a message of the wrong type: %s",
                type(message),
            )
            return

        logger.debug("Received HandshakeResponse %s", message.to_json())

        logger.debug(
            "CEM selected to use version %s", message.selected_protocol_version
        )
        await send_okay
        logger.debug("Handshake complete. Sending first ResourceManagerDetails.")

        await self.send_resource_manager_details()

    async def handle_select_control_type_as_rm(
        self, _: "S2ResourceManagerItem", message: S2Message, send_okay: Awaitable[None]
    ) -> None:
        if not isinstance(message, SelectControlType):
            logger.error(
                "Handler for SelectControlType received a message of the wrong type: %s",
                type(message),
            )
            return

        await send_okay

        logger.debug(
            "CEM selected control type %s. Activating control type.",
            message.control_type,
        )

        control_types_by_protocol_name = {
            c.get_protocol_control_type(): c for c in self.control_types
        }
        selected_control_type: S2ControlType | None = (
            control_types_by_protocol_name.get(message.control_type)
        )

        if self._current_control_type is not None:
            if asyncio.iscoroutinefunction(self._current_control_type.deactivate):
                await self._current_control_type.deactivate(self)
            else:
                await self._runningloop.run_in_executor(
                    None, self._current_control_type.deactivate, self
                )

        self._current_control_type = selected_control_type

        if self._current_control_type is not None:
            if asyncio.iscoroutinefunction(self._current_control_type.activate):
                await self._current_control_type.activate(self)
            else:
                await self._runningloop.run_in_executor(
                    None, self._current_control_type.activate, self
                )
            self._current_control_type.register_handlers(self._handlers)

    async def _send_and_forget(self, s2_msg: S2Message) -> None:
        if not self.is_connected:
            raise RuntimeError(
                "Cannot send messages if client connection is not yet established."
            )

        json_msg = s2_msg.to_json()
        logger.debug("Sending message %s", json_msg)

        try:
            self._send_message(json_msg)
        except Exception as e:
            logger.exception("Unable to send message %s due to %s", s2_msg, str(e))
            self._restart_connection_event.set()

    async def _respond_with_reception_status(
        self, subject_message_id: uuid.UUID, status: ReceptionStatusValues, diagnostic_label: str
    ) -> None:
        logger.debug(
            "Responding to message %s with status %s", subject_message_id, status
        )
        await self._send_and_forget(
            ReceptionStatus(
                subject_message_id=subject_message_id,
                status=status,
                diagnostic_label=diagnostic_label,
            )
        )

    def _respond_with_reception_status_sync(
        self, subject_message_id: uuid.UUID, status: ReceptionStatusValues, diagnostic_label: str
    ) -> None:
        asyncio.run_coroutine_threadsafe(
            self._respond_with_reception_status(
                subject_message_id, status, diagnostic_label
            ),
            self._runningloop,
        ).result()

    async def send_msg_and_await_reception_status(
        self,
        s2_msg: S2Message,
        timeout_reception_status: float = 5.0,
        raise_on_error: bool = True,
    ) -> ReceptionStatus:
        await self._send_and_forget(s2_msg)
        logger.debug(
            "Waiting for ReceptionStatus for %s %s seconds",
            s2_msg.message_id,  # type: ignore[attr-defined, union-attr]
            timeout_reception_status,
        )
        try:
            reception_status = await self.reception_status_awaiter.wait_for_reception_status(
                s2_msg.message_id, timeout_reception_status  # type: ignore[attr-defined, union-attr]
            )
        except TimeoutError:
            logger.error(
                "Did not receive a reception status on time for %s",
                s2_msg.message_id,  # type: ignore[attr-defined, union-attr]
            )
            self._restart_connection_event.set()
            raise

        if reception_status.status != ReceptionStatusValues.OK and raise_on_error:
            raise RuntimeError(
                f"ReceptionStatus was not OK but rather {reception_status.status}"
            )

        return reception_status

    def send_msg_and_await_reception_status_sync(
        self,
        s2_msg: S2Message,
        timeout_reception_status: float = 5.0,
        raise_on_error: bool = True,
    ) -> ReceptionStatus:
        return asyncio.run_coroutine_threadsafe(
            self.send_msg_and_await_reception_status(
                s2_msg, timeout_reception_status, raise_on_error
            ),
            self._runningloop,
        ).result()

    async def _handle_received_messages(self) -> None:
        while True:
            msg = await self._received_messages.get()
            await self._handlers.handle_message(self, msg)

    async def send_resource_manager_details(
        self,
        control_types: List[S2ControlType] = None,
        asset_details: AssetDetails = None
    ) -> ReceptionStatus:
        if control_types is not None:
            self.control_types = control_types
        if asset_details is not None:
            self.asset_details = asset_details
        reception_status: ReceptionStatus = await self.send_msg_and_await_reception_status(
            self.asset_details.to_resource_manager_details(self.control_types)
        )
        return reception_status

    def send_resource_manager_details_sync(
        self,
        control_types: List[S2ControlType] = None,
        asset_details: AssetDetails = None
    ) -> ReceptionStatus:
        return asyncio.run_coroutine_threadsafe(
            self.send_resource_manager_details(
                control_types, asset_details
            ),
            self._runningloop,
        ).result()


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

    def deactivate(self, conn):
        self._active = False
        self._ctrl.items.s2_active = 0
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
