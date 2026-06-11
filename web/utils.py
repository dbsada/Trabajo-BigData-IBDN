"""
Funciones auxiliares para la aplicación web: Cassandra, distancias, fechas.
"""

import datetime
import os
import time

import iso8601


def get_cassandra_session(n_retries=10):
    """
    Conecta con Cassandra y devuelve una sesión.
    Reintenta si Cassandra no está listo.
    """
    session = getattr(get_cassandra_session, "_session", None)
    if session is not None:
        return session

    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([os.getenv("CASSANDRA_HOST", "cassandra")], port=os.getenv("CASSANDRA_PORT", 9042))
        for _ in range(n_retries):
            try:
                session = cluster.connect("agile_data_science")
                get_cassandra_session._session = session
                return session
            except Exception:
                time.sleep(3)
    except Exception:
        pass

    get_cassandra_session._session = None
    return None


def get_flight_distance(origin: str, dest: str):
    """
    Consulta la distancia entre dos aeropuertos en Cassandra.
    Retorna None si no encuentra la ruta.
    """
    session = get_cassandra_session()
    if session is None:
        return None

    row = session.execute(
        "SELECT distance FROM origin_dest_distances WHERE origin=%s AND dest=%s",
        (origin, dest),
    ).one()
    return row.distance if row else None


def get_regression_date_args(iso_date: str) -> dict:
    """
    Convierte una fecha ISO (ej: "2016-12-25") en DayOfYear, DayOfMonth, DayOfWeek.
    Ejemplo de retorno: {"DayOfYear": 360, "DayOfMonth": 25, "DayOfWeek": 6}
    """
    dt = iso8601.parse_date(iso_date)
    return {
        "DayOfYear": dt.timetuple().tm_yday,
        "DayOfMonth": dt.day,
        "DayOfWeek": dt.weekday(),
    }


def get_current_timestamp() -> str:
    """Devuelve el timestamp actual en formato ISO."""
    return datetime.datetime.now().isoformat()