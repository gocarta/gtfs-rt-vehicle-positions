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
from time import sleep
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

while True:
    print(f"[gtfs-rt-vehicle-positions] sleeping {GTFS_UPDATE_FREQUENCY} seconds")
    sleep(GTFS_UPDATE_FREQUENCY)

    feed = gtfs_realtime_pb2.FeedMessage()

    now_datetime = datetime.datetime.now(ZoneInfo(GTFS_TIMEZONE))
    timestamp = int(now_datetime.timestamp())

    # feed header
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = timestamp

    vehicles = client.get_dataset_as_json(name="clever_vehicle_locations", version="1")
    cloud_vehicles = client.get_dataset_as_json(
        name="cloud_vehicle_locations", version="1"
    )

    cloud_vehicles_by_id = dict(
        [(vehicle["vehicle_id"], vehicle) for vehicle in cloud_vehicles]
    )

    cloud_ct = 0
    clever_ct = 0

    for item in vehicles:
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
            vehicle.trip.trip_id = item["trip_id"]  # reluctantly use Clever's trip id
            vehicle.trip.start_time = scheduled_hms
            print("[gtfs-rt-vehicle-positions] couldn't find match for", lookup_key)

        if now_datetime.time() < datetime.time(3, 0):
            # currently assuming don't have any buses that pull out after midnight
            vehicle.trip.start_date = (
                now_datetime.date() - datetime.timedelta(days=1)
            ).strftime("%Y%m%d")
        else:
            vehicle.trip.start_date = now_datetime.date().strftime("%Y%m%d")

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
