import pandas as pd

from ingestion.to_silver import clean_trips, KEEP


def make_raw():
    base = {"VendorID": 2, "tpep_pickup_datetime": "2025-01-10 08:15:00",
            "tpep_dropoff_datetime": "2025-01-10 08:40:00", "passenger_count": 1.0,
            "trip_distance": 3.2, "PULocationID": 132, "DOLocationID": 161,
            "payment_type": 1, "fare_amount": 20.0, "tip_amount": 4.0,
            "tolls_amount": 0.0, "total_amount": 27.0}
    valid = dict(base)
    too_old = dict(base, tpep_pickup_datetime="2009-01-01 10:00:00",
                   tpep_dropoff_datetime="2009-01-01 10:20:00")
    no_passenger = dict(base, passenger_count=None)
    return pd.DataFrame([valid, too_old, no_passenger])


def test_rows_outside_month_are_removed():
    out = clean_trips(make_raw(), 2025, 1)
    assert len(out) == 2            # la course de 2009 a disparu


def test_columns_and_types():
    out = clean_trips(make_raw(), 2025, 1)
    assert list(out.columns) == KEEP
    assert str(out["passenger_count"].dtype) == "int32"
    assert str(out["fare_amount"].dtype) == "float64"


def test_missing_passenger_becomes_zero():
    out = clean_trips(make_raw(), 2025, 1)
    assert (out["passenger_count"] == 0).sum() == 1