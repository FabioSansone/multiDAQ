
def handle_server_shutdown(manager, message):
    manager.logger.warning(
        f"Server shutdown received: {message.payload}"
    )
    
    manager.runtime.evproducer.stop()

    manager.reconnect_requested.set()
    manager.stop_listening.set()

    manager.logger.info("Reconnect requested after server shutdown")