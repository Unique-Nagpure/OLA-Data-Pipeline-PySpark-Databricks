"""
generate_sample_data.py
------------------------
Generates synthetic OLA-style ride-hailing data across three raw file
formats (CSV, JSON, Parquet) to simulate files landing in an ADLS
container. Deliberately injects a handful of dirty/invalid records so
the Data Quality Check and Data Validation notebooks have something
real to catch.

Run locally: python scripts/generate_sample_data.py
Requires: pandas, pyarrow (pip install -r requirements.txt)
"""

import json
import random
from datetime import datetime, timedelta
import pandas as pd

random.seed(42)

OUT = "data/raw"

CITIES = ["Indore", "Pune", "Mumbai", "Bengaluru", "Delhi", "Hyderabad", "Chennai"]
VEHICLE_TYPES = ["Micro", "Mini", "Prime", "SUV", "Auto", "Bike"]
PAYMENT_TYPES = ["UPI", "Cash", "Card", "Wallet"]
FIRST_NAMES = ["Aditi", "Rohan", "Sneha", "Karan", "Priya", "Arjun", "Neha", "Vikram",
               "Farhan", "Meera", "Sanjay", "Divya", "Rahul", "Anjali", "Suresh", "Pooja"]
LAST_NAMES = ["Sharma", "Mehta", "Patil", "Verma", "Nair", "Rao", "Joshi", "Singh",
              "Ali", "Iyer", "Kumar", "Gupta", "Reddy", "Kapoor"]


def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def rand_date(start_year=2022, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Customers (CSV)
# ---------------------------------------------------------------------------
def generate_customers(n=40):
    rows = []
    for i in range(1, n + 1):
        name = rand_name()
        rows.append({
            "customer_id": f"CUST{i:04d}",
            "name": name,
            "email": f"{name.lower().replace(' ', '.')}{i}@example.com",
            "phone": f"9{random.randint(100000000, 999999999)}",
            "city": random.choice(CITIES),
            "signup_date": rand_date(2022, 2023),
        })
    df = pd.DataFrame(rows)
    df.to_csv(f"{OUT}/customers/customers.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# Vehicles (Parquet)
# ---------------------------------------------------------------------------
MAKES_MODELS = [
    ("Maruti Suzuki", "Swift"), ("Maruti Suzuki", "Dzire"), ("Hyundai", "i10"),
    ("Hyundai", "Aura"), ("Toyota", "Innova"), ("Mahindra", "XUV300"),
    ("Honda", "Amaze"), ("Tata", "Tigor"), ("Bajaj", "RE Auto"), ("TVS", "Apache"),
]


def generate_vehicles(n=25):
    rows = []
    for i in range(1, n + 1):
        make, model = random.choice(MAKES_MODELS)
        rows.append({
            "vehicle_id": f"VEH{i:04d}",
            "make": make,
            "model": model,
            "vehicle_type": random.choice(VEHICLE_TYPES),
            "registration_number": f"MP09{random.choice('ABCDEFGH')}{random.randint(1000, 9999)}",
            "manufacture_year": random.randint(2017, 2023),
        })
    df = pd.DataFrame(rows)
    df.to_parquet(f"{OUT}/vehicles/vehicles.parquet", index=False)
    return df


# ---------------------------------------------------------------------------
# Drivers (JSON)
# ---------------------------------------------------------------------------
def generate_drivers(vehicle_ids, n=25):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "driver_id": f"DRV{i:04d}",
            "name": rand_name(),
            "license_number": f"DL{random.randint(1000000000, 9999999999)}",
            "rating": round(random.uniform(3.5, 5.0), 1),
            "city": random.choice(CITIES),
            "join_date": rand_date(2021, 2023),
            "vehicle_id": vehicle_ids[i - 1],
        })
    with open(f"{OUT}/drivers/drivers.json", "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return rows


# ---------------------------------------------------------------------------
# Locations (CSV)
# ---------------------------------------------------------------------------
AREA_NAMES = [
    "Vijay Nagar", "Palasia", "Rajwada", "MG Road", "Koramangala", "Andheri",
    "Bandra", "Hinjewadi", "Kothrud", "Banjara Hills", "Hitech City",
    "T Nagar", "Anna Nagar", "Connaught Place", "Saket", "Whitefield",
    "Indiranagar", "Powai",
]


def generate_locations():
    rows = []
    for i, area in enumerate(AREA_NAMES, start=1):
        rows.append({
            "location_id": f"LOC{i:03d}",
            "area_name": area,
            "city": random.choice(CITIES),
        })
    df = pd.DataFrame(rows)
    df.to_csv(f"{OUT}/locations/locations.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# Trips (JSON, two incremental batch files) — includes deliberately dirty rows
# ---------------------------------------------------------------------------
def generate_trips(customer_ids, driver_ids, vehicle_ids, location_ids, per_batch=150):
    trip_counter = 1

    def make_trip(month):
        nonlocal trip_counter
        trip_id = f"TRIP{trip_counter:06d}"
        trip_counter += 1
        pickup_day = random.randint(1, 28)
        pickup_hour = random.randint(0, 23)
        pickup_dt = datetime(2024, month, pickup_day, pickup_hour, random.randint(0, 59))
        duration_min = random.randint(5, 60)
        drop_dt = pickup_dt + timedelta(minutes=duration_min)
        distance = round(random.uniform(1.0, 25.0), 1)
        fare = round(distance * random.uniform(10, 18) + random.uniform(10, 40), 2)
        status = random.choices(["COMPLETED", "CANCELLED"], weights=[0.85, 0.15])[0]
        return {
            "trip_id": trip_id,
            "customer_id": random.choice(customer_ids),
            "driver_id": random.choice(driver_ids),
            "vehicle_id": random.choice(vehicle_ids),
            "pickup_location_id": random.choice(location_ids),
            "drop_location_id": random.choice(location_ids),
            "pickup_ts": pickup_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "drop_ts": drop_dt.strftime("%Y-%m-%dT%H:%M:%S") if status == "COMPLETED" else None,
            "distance_km": distance if status == "COMPLETED" else 0.0,
            "fare_amount": fare if status == "COMPLETED" else 0.0,
            "payment_type": random.choice(PAYMENT_TYPES),
            "status": status,
            "customer_rating": random.randint(1, 5) if status == "COMPLETED" else None,
        }

    for batch_num, month in enumerate([1, 2], start=1):
        rows = [make_trip(month) for _ in range(per_batch)]

        # --- inject dirty records for DQ / validation notebooks to catch ---
        if batch_num == 1:
            rows[5]["customer_id"] = None                       # missing FK
            rows[6]["driver_id"] = None                          # missing FK
            rows[10]["fare_amount"] = -250.0                     # negative fare
            rows[11]["distance_km"] = -3.5                       # negative distance
            rows[15]["status"] = "UNKNOWN_STATUS"                 # invalid enum
            rows[20]["customer_id"] = "CUST9999"                 # orphan FK (no such customer)
            rows.append(dict(rows[0]))                           # exact duplicate trip_id
        else:
            rows[8]["driver_id"] = "DRV9999"                     # orphan FK
            rows[9]["fare_amount"] = -75.0
            drop_before_pickup = rows[30]
            pickup_dt = datetime.strptime(drop_before_pickup["pickup_ts"], "%Y-%m-%dT%H:%M:%S")
            drop_before_pickup["drop_ts"] = (pickup_dt - timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%S")
            rows[40]["customer_rating"] = 9                      # out-of-range rating

        with open(f"{OUT}/trips/trips_2024_{month:02d}.json", "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")


def main():
    customers = generate_customers()
    vehicles = generate_vehicles()
    drivers = generate_drivers(vehicle_ids=list(vehicles["vehicle_id"]))
    locations = generate_locations()
    generate_trips(
        customer_ids=list(customers["customer_id"]),
        driver_ids=[d["driver_id"] for d in drivers],
        vehicle_ids=list(vehicles["vehicle_id"]),
        location_ids=list(locations["location_id"]),
    )
    print("Sample data generated under data/raw/")


if __name__ == "__main__":
    main()
