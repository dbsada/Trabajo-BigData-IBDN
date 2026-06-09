import sys, os, re
import time
import datetime, iso8601

def get_cassandra_session():
  if not hasattr(get_cassandra_session, '_session'):
    try:
      from cassandra.cluster import Cluster
      cluster = Cluster(['cassandra'], port=9042)
      for _ in range(5):
        try:
          get_cassandra_session._session = cluster.connect('agile_data_science')
          break
        except Exception:
          time.sleep(2)
      if not hasattr(get_cassandra_session, '_session'):
        get_cassandra_session._session = None
    except Exception:
      get_cassandra_session._session = None
  return get_cassandra_session._session

def get_flight_distance(origin, dest):
  session = get_cassandra_session()
  if session:
    row = session.execute(
      "SELECT distance FROM origin_dest_distances WHERE origin=%s AND dest=%s",
      (origin, dest)
    ).one()
    if row:
      return row.distance
  return None

def get_regression_date_args(iso_date):
  """Given an ISO Date, return the day of year, day of month, day of week as the API expects them."""
  dt = iso8601.parse_date(iso_date)
  day_of_year = dt.timetuple().tm_yday
  day_of_month = dt.day
  day_of_week = dt.weekday()
  return {
    "DayOfYear": day_of_year,
    "DayOfMonth": day_of_month,
    "DayOfWeek": day_of_week,
  }

def get_current_timestamp():
  iso_now = datetime.datetime.now().isoformat()
  return iso_now
