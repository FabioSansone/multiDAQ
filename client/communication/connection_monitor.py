import threading
import zmq
from zmq.utils.monitor import recv_monitor_message

from client.utils.logger import get_logger

class ConnectionMonitor:

    def __init__(self, socket, name, on_disconnected):

        self.logger = get_logger('connection_monitor')
        self.logger.debug(f"Connection Monitor '{name}' initialised")

        self._name = name
        self._socket = socket
        self._on_disconnected = on_disconnected

        self._stop = threading.Event()
        self._fired = False

        self._monitor = socket.get_monitor_socket(zmq.EVENT_DISCONNECTED)
        self._thread = threading.Thread(
            target=self._run,
            name=f"connection-monitor-{name}",
            daemon=True,
        )


    def start(self):
        self._thread.start()
        self.logger.info(f"Connection monitor '{self._name}' started")


    def _run(self):
        while not self._stop.is_set():
            if not self._monitor.poll(timeout=500):
                continue
            event = recv_monitor_message(socket=self._monitor)
            if event["event"] == zmq.EVENT_DISCONNECTED and not self._fired:
                self._fired = True
                self.logger.warning(f"Connection '{self._name}' lost: {event.get('endpoint')}")
                try:
                    self._on_disconnected()
                except Exception:
                    self.logger.exception(f"Disconnection callback of '{self._name}' failed")


    def stop(self):
        """Safe to call twice, before start() and from the callback itself."""
        self._stop.set()

        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(2.0)

        try:
            self._socket.disable_monitor()
        except zmq.ZMQError as exc:
            self.logger.debug(f"Monitor of '{self._name}' already disabled: {exc}")

        try:
            self._monitor.close()
        except zmq.ZMQError as exc:
            self.logger.debug(f"Monitor socket of '{self._name}' already closed: {exc}")

        self.logger.info(f"Connection monitor '{self._name}' stopped")
