# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "boto3",
#     "datablob",
#     "gtfs-realtime-bindings",
#     "simple-env",
#     "tzdata",
# ]
# ///
import boto3
import datablob
import datetime
from google.transit import gtfs_realtime_pb2
import simple_env as se
from time import sleep, perf_counter
from zoneinfo import ZoneInfo

AWS_BUCKET_NAME = se.get("AWS_BUCKET_NAME")
if not AWS_BUCKET_NAME:
    raise Exception("[gtfs-rt-vehicle-positions] missing AWS_BUCKET_NAME")

AWS_BUCKET_PATH = se.get("AWS_BUCKET_PATH")
if not AWS_BUCKET_PATH:
    raise Exception("[gtfs-rt-vehicle-positions] missing AWS_BUCKET_PATH")

AWS_REGION = se.get("AWS_REGION")
if not AWS_REGION:
    raise Exception("[gtfs-rt-vehicle-positions] missing AWS_REGION")

GTFS_TIMEZONE = se.get("GTFS_TIMEZONE")
if not GTFS_TIMEZONE:
    raise Exception("[gtfs-rt-vehicle-positions] missing GTFS_TIMEZONE")

GTFS_UPDATE_FREQUENCY = se.get("GTFS_UPDATE_FREQUENCY")
if not GTFS_UPDATE_FREQUENCY:
    raise Exception("[gtfs-rt-vehicle-positions] missing GTFS_UPDATE_FREQUENCY")

direction_ids = {"0": "Outbound", "1": "Inbound"}

extras = [{"vehicle_id": "739", "route_id": "34"}]


def hms(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


client = datablob.DataBlobClient(
    bucket_name=AWS_BUCKET_NAME, bucket_path=AWS_BUCKET_PATH
)

scheduled_bus_trips = client.get_dataset_as_json(
    name="scheduled_bus_trips", version="1"
)

trips_lookup = {}

for trip in scheduled_bus_trips:
    route_id = trip["route_id"]
    gtfs_headsign = trip["headsign"]
    start_hours, start_minutes, start_seconds = trip["start_time"].split(":")
    start_time = (
        int(start_hours) * 60 * 60 + int(start_minutes) * 60 + int(start_seconds)
    )
    key = (route_id, gtfs_headsign, start_time)
    trips_lookup[key] = trip


def debouncer(wait_seconds):
    last_called = {"time": 0.0}

    def inner():
        now = perf_counter()
        elapsed = now - last_called["time"]

        if elapsed < wait_seconds:
            print("sleeping", wait_seconds - elapsed, "seconds")
            sleep(wait_seconds - elapsed)

        last_called["time"] = perf_counter()
        return last_called["time"]

    return inner


debounce = debouncer(GTFS_UPDATE_FREQUENCY)

while True:
    debounce()

    rows = []

    feed = gtfs_realtime_pb2.FeedMessage()

    now_datetime = datetime.datetime.now(ZoneInfo(GTFS_TIMEZONE))
    timestamp = int(now_datetime.timestamp())

    # feed header
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = timestamp

    clever_vehicles = client.get_dataset_as_json(
        name="clever_vehicle_locations", version="1"
    )
    clever_vehicle_ids = set([vehicle["vehicle_id"] for vehicle in clever_vehicles])

    cloud_vehicles = client.get_dataset_as_json(
        name="cloud_vehicle_locations", version="1"
    )

    cloud_vehicles_by_id = dict(
        [(vehicle["vehicle_id"], vehicle) for vehicle in cloud_vehicles]
    )

    cloud_ct = 0
    clever_ct = 0

    for item in clever_vehicles:
        # print("\n\nclever vehicle:\n", item)
        vehicle_id = item["vehicle_id"]
        route_id = item["route"]

        entity = feed.entity.add()
        entity.id = vehicle_id

        vehicle = entity.vehicle
        vehicle.vehicle.id = vehicle_id
        vehicle.vehicle.label = vehicle_id

        clever_vehicle_datetime = datetime.datetime.fromisoformat(item["timestamp"])

        if vehicle_id in cloud_vehicles_by_id:
            cloud_vehicle = cloud_vehicles_by_id[vehicle_id]

            cloud_vehicle_datetime = datetime.datetime.fromisoformat(
                cloud_vehicle["reported"]
            )

            # pick whatever has the most recent gps ping
            if cloud_vehicle_datetime > clever_vehicle_datetime:
                vehicle.position.latitude = cloud_vehicle["latitude"]
                vehicle.position.longitude = cloud_vehicle["longitude"]
                vehicle.timestamp = int(cloud_vehicle_datetime.timestamp())
                cloud_ct += 1
            else:
                vehicle.position.latitude = item["latitude"]
                vehicle.position.longitude = item["longitude"]
                vehicle.timestamp = int(clever_vehicle_datetime.timestamp())
                clever_ct += 1
        else:
            vehicle.position.latitude = item["latitude"]
            vehicle.position.longitude = item["longitude"]
            vehicle.timestamp = int(clever_vehicle_datetime.timestamp())
            clever_ct += 1

        # skip if invalid position
        if vehicle.position.latitude == 0 or vehicle.position.longitude == 0:
            feed.entity.pop()
            continue

        if "heading" in item:
            vehicle.position.bearing = float(item["heading"])
        if "speed" in item:
            # convert miles per hour to meters per second
            speed = float(item["speed"]) * 0.44704
            if 0 < speed < 26:
                # only add speed if it's realistic
                vehicle.position.speed = speed

        route = item["route"]

        # when the trip was scheduled to start
        scheduled_start_time = item["scheduled_start_time"]
        scheduled_hms = hms(scheduled_start_time)

        clever_destination = item["destination"]

        lookup_key = (route_id, clever_destination, scheduled_start_time)
        # print("lookup_key:", lookup_key)

        vehicle.trip.route_id = route_id
        vehicle.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.SCHEDULED

        if lookup_key in trips_lookup:
            gtfs_scheduled_trip = trips_lookup[lookup_key]
            # print("\nmatch:\n", gtfs_scheduled_trip, "\n\n")
            vehicle.trip.trip_id = gtfs_scheduled_trip["trip_id"]
            vehicle.trip.start_time = gtfs_scheduled_trip["start_time"]
            vehicle.trip.direction_id = int(gtfs_scheduled_trip["direction_id"])
            # vehicle.stop_id = ... # can look up in static GTFS stop_times, to get sequence
        else:
            gtfs_scheduled_trip = None
            vehicle.trip.trip_id = item["trip_id"]  # reluctantly use Clever's trip id
            vehicle.trip.start_time = scheduled_hms
            print("[gtfs-rt-vehicle-positions] couldn't find match for", lookup_key)

        if now_datetime.time() < datetime.time(3, 0):
            # currently assuming don't have any buses that pull out after midnight
            trip_start_date = now_datetime.date() - datetime.timedelta(days=1)
        else:
            trip_start_date = now_datetime.date()
        vehicle.trip.start_date = trip_start_date.strftime("%Y%m%d")

        rows.append(
            {
                "vehicle_id": vehicle_id,
                "route_id": route_id,
                "trip_id": vehicle.trip.trip_id,
                "trip_start_date": trip_start_date.strftime("%Y-%m-%d"),
                "trip_start_time": vehicle.trip.start_time,
                "direction_id": vehicle.trip.direction_id,
                "direction": direction_ids[str(vehicle.trip.direction_id)],
                "headsign": gtfs_scheduled_trip["headsign"]
                if gtfs_scheduled_trip
                else None,
                "latitude": vehicle.position.latitude,
                "longitude": vehicle.position.longitude,
                "speed": vehicle.position.speed,
                "bearing": vehicle.position.bearing,
                "timestamp": vehicle.timestamp,
                "schedule_relationship": "scheduled",
            }
        )

    # adding extras that aren't included in Clever
    for extra in extras:
        vehicle_id = extra["vehicle_id"]
        route_id = extra["route_id"]
        reported = cloud_vehicle["reported"]

        cloud_vehicle = cloud_vehicles_by_id[vehicle_id]

        cloud_vehicle_datetime = datetime.datetime.fromisoformat(reported)

        if not (
            now_datetime - datetime.timedelta(minutes=1)
            <= cloud_vehicle_datetime
            <= now_datetime + datetime.timedelta(minutes=1)
        ):
            print("[dataops-gtfs-rt-vehicle-positions] timestamp is too old")
            continue

        if vehicle_id in clever_vehicle_ids:
            print(
                "[dataops-gtfs-rt-vehicle-positions] vehicle already processed using clever list"
            )
            continue

        if now_datetime.time() < datetime.time(3, 0):
            # currently assuming don't have any buses that pull out after midnight
            trip_start_date = now_datetime.date() - datetime.timedelta(days=1)
        else:
            trip_start_date = now_datetime.date()

        rows.append(
            {
                "vehicle_id": vehicle_id,
                "route_id": route_id,
                "trip_start_date": trip_start_date.strftime("%Y-%m-%d"),
                "latitude": cloud_vehicle["latitude"],
                "longitude": cloud_vehicle["longitude"],
                "timestamp": int(
                    datetime.datetime.fromisoformat(
                        cloud_vehicle["reported"]
                    ).timestamp()
                ),
                "schedule_relationship": "scheduled",
            }
        )
        print(f"[gtfs-rt-vehicle-positions] added extra vehicle: {vehicle_id}")

    print(f"[gtfs-rt-vehicle-positions] used cloud for vehicle positions: {cloud_ct}")
    print(f"[gtfs-rt-vehicle-positions] used clever for vehicle positions: {clever_ct}")

    result = feed.SerializeToString()

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket="gocarta",
        Key="public/gtfs-rt/VehiclePositions.pb",
        Body=result,
        ContentType="application/x-protobuf",
        # Adding CacheControl prevents clients from seeing stale bus locations
        CacheControl="max-age=0, no-cache, no-store, must-revalidate",
    )
    print(f"[gtfs-rt-vehicle-positions] updated GTFS Realtime feed")

    client = datablob.DataBlobClient(
        bucket_name=AWS_BUCKET_NAME, bucket_path=AWS_BUCKET_PATH
    )

    client.update_dataset(
        name="gtfsrt_vehicle_positions",
        description="GTFS Realtime Vehicle Positions.  The location of all CARTA Buses and Shuttles, created by fusing multiple data streams.",
        version="1",
        data=rows,
        column_names=[
            "vehicle_id",
            "route_id",
            "trip_id",
            "trip_start_date",
            "trip_start_time",
            "direction_id",
            "direction",
            "headsign",
            "latitude",
            "longitude",
            "speed",
            "bearing",
            "timestamp",
            "schedule_relationship",
        ],
        latitude_key="latitude",
        longitude_key="longitude",
    )
    print("[dataops-gtfsrt-vehicle-positions] updated dataset")

    # geojson = client.convert_rows_to_geojson_points(
    #     rows=rows, longitude_key="longitude", latitude_key="latitude"
    # )
    # client.upload_geojson_points(
    #     dataset_name="gtfsrt_vehicle_positions", dataset_version="1", data=geojson
    # )

    # metadata = {
    #     "name": "gtfsrt_vehicle_positions",
    #     "lastUpdated": dict(
    #         [
    #             (tz, datetime.datetime.now(ZoneInfo(tz)).isoformat())
    #             for tz in ["UTC", "America/New_York"]
    #         ]
    #     ),
    #     "description": "GTFS Realtime Vehicle Positions.  The location of all CARTA Buses and Shuttles, created by fusing multiple data streams.",
    #     "numColumns": 12,
    #     "numRows": len(rows),
    #     "columns": [
    #         "vehicle_id",
    #         "route_id",
    #         "trip_id",
    #         "trip_start_date",
    #         "trip_start_time",
    #         "direction_id",
    #         "latitude",
    #         "longitude",
    #         "speed",
    #         "bearing",
    #         "timestamp",
    #         "schedule_relationship",
    #     ],
    #     "files": [{"filename": "data.points.geojson", "format": "GeoJSON (Points)"}],
    # }
    # client.upload_metadata("gtfsrt_vehicle_positions", "1", metadata)

    # print(f"[gtfs-rt-vehicle-positions] uploaded geojson")
