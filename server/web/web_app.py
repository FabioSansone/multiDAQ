import argparse
import os
import threading
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

from common.constants import ACQUISITION_MODES
from server.commands.hv_commands import COMMAND_FSM_MAP as HV_FSM_MAP
from server.commands.rc_commands import COMMAND_FSM_MAP as RC_FSM_MAP
from server.utils.logger import get_logger


DEFAULT_CONTROL_PORT = 8888
DEFAULT_ACQ_PORT = 8889


class WebControlPanel:
    def __init__(self, server) -> None:
        self.server = server
        self.logger = get_logger("web")
        self.lock = threading.RLock()

    def _client_name(self, client_id: bytes) -> str:
        return client_id.decode(errors="ignore")

    def _json_error(self, message: str, status_code: int = 400):
        response = jsonify({"ok": False, "error": message})
        response.status_code = status_code
        return response

    def status(self) -> dict[str, Any]:
        server_state = self.server.server_state
        control_clients = server_state.list_connected_clients()
        acq_clients = server_state.list_acquisition_clients()

        clients = []
        for client_id in control_clients:
            identity = server_state.get_identity(client_id) or {}
            client_state = server_state.get_client_state(client_id)
            clients.append(
                {
                    "id": self._client_name(client_id),
                    "identity": identity,
                    "control_connected": True,
                    "acquisition_connected": client_id in acq_clients,
                    "fsm_state": client_state.value if client_state else "unknown",
                }
            )

        receiver = self.server.data_receiver_service.status()

        return {
            "mode": self.server.mode,
            "prompt": self.server.prompt,
            "run_state": server_state.get_server_state().value,
            "clients": clients,
            "control": {
                "endpoint": self.server.control_manager.endpoint,
                "listener_running": (
                    self.server.control_manager.listener_thread is not None
                    and self.server.control_manager.listener_thread.is_alive()
                ),
            },
            "acquisition": {
                "endpoint": self.server.acq_manager.endpoint,
                "listener_running": (
                    self.server.acq_manager.acq_listener_thread is not None
                    and self.server.acq_manager.acq_listener_thread.is_alive()
                ),
                "clients": [self._client_name(cid) for cid in acq_clients],
                "receiver": receiver,
            },
        }

    def connect_clients(self, payload: dict[str, Any]) -> dict[str, Any]:
        args = argparse.Namespace(
            num_clients=int(payload.get("num_clients", 1)),
            control_port=int(payload.get("control_port", DEFAULT_CONTROL_PORT)),
            acq_port=int(payload.get("acq_port", DEFAULT_ACQ_PORT)),
        )
        with self.lock:
            self.server.do_connect(args)
        return self.status()

    def change_mode(self, payload: dict[str, Any]) -> dict[str, Any]:
        new_mode = str(payload.get("mode", "")).lower()
        if new_mode not in ACQUISITION_MODES:
            raise ValueError(f"Invalid acquisition mode: {new_mode}")

        args = argparse.Namespace(mode=new_mode)
        with self.lock:
            self.server.do_change_mode(args)
        return self.status()

    def _check_hv_state(self, command_group: str) -> None:
        allowed = HV_FSM_MAP.get(command_group)
        if allowed is None:
            raise ValueError(f"Unknown HV command group: {command_group}")
        current = self.server.server_state.get_server_state()
        if current not in allowed:
            allowed_names = ", ".join(sorted(s.value for s in allowed))
            raise RuntimeError(
                f"Cannot run 'hv {command_group}' while server is "
                f"'{current.value}'. Allowed states: {allowed_names}."
            )

    def send_hv_command(self, payload: dict[str, Any]) -> dict[str, Any]:
        from server.services.client_command_service import CommandPlane

        command = str(payload.get("command", ""))
        group_map = {
            "hv_on": "on", "hv_off": "off", "set_hv_sync": "on",
            "set_common_voltage": "set_common", "set_common_threshold": "set_common",
        }
        self._check_hv_state(group_map.get(command, command))

        channels = payload.get("channels", "all")
        command_payload: dict[str, Any] = {"channels": channels}
        if command == "set_common_voltage":
            command_payload["common_voltage"] = int(payload["value"])
        elif command == "set_common_threshold":
            command_payload["common_threshold"] = int(payload["value"])
        elif command not in {"hv_on", "hv_off", "set_hv_sync"}:
            raise ValueError(f"Unsupported HV command: {command}")

        client_ids = self.server.server_state.list_connected_clients()
        if not client_ids:
            raise RuntimeError("No connected clients")

        replies = []
        with self.lock:
            for client_id in client_ids:
                reply, reason = self.server.client_command_service.send_hv_command(
                    client_id=client_id,
                    command=command,
                    payload=command_payload,
                    plane=CommandPlane.CONTROL,
                    timeout_s=90.0,
                )
                replies.append(self._format_reply(client_id, reply, reason))

        return {"ok": True, "replies": replies}

    def send_rc_command(self, payload: dict[str, Any]) -> dict[str, Any]:
        from server.services.client_command_service import CommandPlane

        command = str(payload.get("command", ""))
        group_map = {
            "rc_acq_start": "start", "rc_reset": "reset", "rc_boot": "boot",
            "rc_read_register": "read", "rc_write_register": "write",
        }
        allowed = RC_FSM_MAP.get(group_map.get(command, command))
        if allowed is None:
            raise ValueError(f"Unsupported RC command: {command}")
        current = self.server.server_state.get_server_state()
        if current not in allowed:
            allowed_names = ", ".join(sorted(s.value for s in allowed))
            raise RuntimeError(
                f"Cannot run this RC command while server is '{current.value}'. "
                f"Allowed states: {allowed_names}."
            )

        client_ids = self.server.server_state.list_connected_clients()
        if not client_ids:
            raise RuntimeError("No connected clients")

        if command in {"rc_acq_start", "rc_reset", "rc_boot"}:
            command_payload = {"channels": payload.get("channels", "all")}
        elif command == "rc_read_register":
            command_payload = {"address": int(payload["address"])}
        elif command == "rc_write_register":
            command_payload = {
                "address": int(payload["address"]),
                "value": int(payload["value"]),
            }
        else:
            raise ValueError(f"Unsupported RC command: {command}")

        replies = []
        with self.lock:
            for client_id in client_ids:
                reply, reason = self.server.client_command_service.send_rc_command(
                    client_id=client_id,
                    command=command,
                    payload=command_payload,
                    plane=CommandPlane.CONTROL,
                    timeout_s=35.0,
                )
                replies.append(self._format_reply(client_id, reply, reason))

        return {"ok": True, "replies": replies}

    def acquisition_start(self, payload: dict[str, Any]) -> dict[str, Any]:
        args = argparse.Namespace(
            duration=_optional_float(payload.get("duration")),
            acq_type=str(payload.get("type") or "test"),
            suffix=str(payload.get("suffix") or ""),
            run_id=payload.get("run_id") or None,
            batch_id=payload.get("batch_id") or None,
            force_compile=bool(payload.get("force_compile", False)),
        )
        with self.lock:
            self.server.acquisition_orchestrator.start(args)
        return self.status()

    def acquisition_stop(self) -> dict[str, Any]:
        with self.lock:
            self.server.acquisition_orchestrator.stop()
        return self.status()

    def shutdown(self, force: bool = False) -> dict[str, Any]:
        with self.lock:
            if force:
                self.server.do_force(argparse.Namespace(command="quit"))
            else:
                self.server.do_quit(None)
        return {"ok": True}

    def _format_reply(self, client_id: bytes, reply, reason: str) -> dict[str, Any]:
        if reply is None:
            return {"client": self._client_name(client_id), "ok": False, "error": reason}
        payload = reply.payload or {}
        return {
            "client": self._client_name(client_id),
            "ok": payload.get("status") == "ok" and not payload.get("error"),
            "payload": payload,
        }


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _json_payload() -> dict[str, Any]:
    if not request.is_json:
        return {}
    return request.get_json(silent=True) or {}


def create_app(server, grafana_url: str | None = None) -> Flask:
    template_dir = Path(__file__).parent / "templates"
    static_dir = Path(__file__).parent / "static"
    app = Flask(__name__, template_folder=str(template_dir), static_folder=str(static_dir))
    panel = WebControlPanel(server)

    resolved_grafana_url = (
        grafana_url or os.environ.get("MULTIDAQ_GRAFANA_URL") or "http://localhost:3000"
    )

    @app.get("/")
    def index():
        return render_template("index.html", grafana_url=resolved_grafana_url)

    @app.get("/api/status")
    def api_status():
        return jsonify({"ok": True, "status": panel.status()})

    @app.post("/api/connect")
    def api_connect():
        try:
            return jsonify({"ok": True, "status": panel.connect_clients(_json_payload())})
        except Exception as exc:
            panel.logger.error(f"Web connect failed: {exc}")
            return panel._json_error(str(exc), 500)

    @app.post("/api/mode")
    def api_mode():
        try:
            return jsonify({"ok": True, "status": panel.change_mode(_json_payload())})
        except Exception as exc:
            panel.logger.error(f"Web mode change failed: {exc}")
            return panel._json_error(str(exc), 500)

    @app.post("/api/hv")
    def api_hv():
        try:
            return jsonify(panel.send_hv_command(_json_payload()))
        except Exception as exc:
            panel.logger.error(f"Web HV command failed: {exc}")
            return panel._json_error(str(exc), 500)

    @app.post("/api/rc")
    def api_rc():
        try:
            return jsonify(panel.send_rc_command(_json_payload()))
        except Exception as exc:
            panel.logger.error(f"Web RC command failed: {exc}")
            return panel._json_error(str(exc), 500)

    @app.post("/api/acquisition/start")
    def api_acquisition_start():
        try:
            return jsonify({"ok": True, "status": panel.acquisition_start(_json_payload())})
        except Exception as exc:
            panel.logger.error(f"Web acquisition start failed: {exc}")
            return panel._json_error(str(exc), 500)

    @app.post("/api/acquisition/stop")
    def api_acquisition_stop():
        try:
            return jsonify({"ok": True, "status": panel.acquisition_stop()})
        except Exception as exc:
            panel.logger.error(f"Web acquisition stop failed: {exc}")
            return panel._json_error(str(exc), 500)

    @app.post("/api/shutdown")
    def api_shutdown():
        try:
            payload = _json_payload()
            return jsonify(panel.shutdown(force=bool(payload.get("force", False))))
        except Exception as exc:
            panel.logger.error(f"Web shutdown failed: {exc}")
            return panel._json_error(str(exc), 500)

    return app


def start_web_server(server, host: str, port: int, grafana_url: str | None = None) -> threading.Thread:
    app = create_app(server, grafana_url=grafana_url)
    logger = get_logger("web")

    def run() -> None:
        logger.info(f"Starting Flask web panel on http://{host}:{port}")
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread