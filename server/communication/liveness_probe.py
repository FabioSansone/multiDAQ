import time

import zmq

from common.timeouts import LIVENESS_PROBE_INTERVAL_S


class LivenessProbe:
    """
    Periodic probe of the clients registered on a plane, run by the I/O thread that owns the socket.

    The probe is an empty frame, which the clients ignore. The ROUTER socket has
    ROUTER_MANDATORY, so sending to an identity whose connection is gone fails with
    EHOSTUNREACH: that is how a vanished client is noticed when nothing else is sent to it.
    """

    def __init__(self, logger, interval_s: float = LIVENESS_PROBE_INTERVAL_S) -> None:
        self.logger = logger
        self.interval_s = interval_s
        self._next_probe = time.monotonic() + interval_s

    def run_if_due(self, socket, client_ids, notify_lost) -> None:
        now = time.monotonic()

        if now < self._next_probe:
            return

        self._next_probe = now + self.interval_s

        for client_id in client_ids:
            try:
                socket.send_multipart([client_id, b""], flags=zmq.DONTWAIT)

            except zmq.ZMQError as exc:
                if exc.errno == zmq.EHOSTUNREACH:
                    self.logger.warning(f"Liveness probe: client {client_id!r} unreachable")
                    notify_lost(client_id)
                elif exc.errno != zmq.EAGAIN:      # EAGAIN: the pipe is full, the client is slow, not gone
                    self.logger.error(f"Liveness probe to client {client_id!r} failed: {exc}")
