"""
Risoluzione della priorità per i comandi RC/HV in arrivo dal server.

Regola: se il server specifica esplicitamente 'priority' nel messaggio,
quella vince sempre. Altrimenti si deduce dal piano di trasporto da cui
il comando è arrivato (manager.plane_name).
"""

PLANE_DEFAULT_PRIORITY = {
    "control": 1,       # CONTROL
    "acquisition": 2,   # ACQUISITION
    "monitoring": 3,    # MONITORING
}


def resolve_priority_value(manager, message) -> int:
    if message.priority is not None:
        return message.priority
    return PLANE_DEFAULT_PRIORITY.get(manager.plane_name, 1)