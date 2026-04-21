# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "boto3",
#     "datablob",
#     "gtfs-realtime-bindings",
#     "requests",
#     "simple-env",
# ]
#
# [tool.uv.sources]
# datablob = { path = "../datablob", editable = true }
# ///
import boto3
import datablob
import datetime
from google.transit import gtfs_realtime_pb2
import simple_env as se

AWS_BUCKET_NAME = se.get("AWS_BUCKET_NAME")
AWS_BUCKET_PATH = se.get("AWS_BUCKET_PATH")

client = datablob.DataBlobClient(
    bucket_name=AWS_BUCKET_NAME, bucket_path=AWS_BUCKET_PATH
)

vehicles = client.get_dataset_as_json(name="clever_vehicle_locations", version="1")

feed = gtfs_realtime_pb2.FeedMessage()

# feed header
feed.header.gtfs_realtime_version = "2.0"
feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
feed.header.timestamp = int(datetime.datetime.now().timestamp())

for item in vehicles:
    entity = feed.entity.add()
    entity.id = item['vehicle_id']
    
    vehicle = entity.vehicle
    vehicle.vehicle.id = item['vehicle_id']
    vehicle.trip.route_id = item['route']
    vehicle.trip.trip_id = str(int(item['trip_id']))
    vehicle.position.latitude = item['latitude']
    vehicle.position.longitude = item['longitude']
    if "heading" in item:
        vehicle.position.bearing = float(item['heading'])
    if "speed" in item:
        vehicle.position.speed = float(item['speed']) * 0.44704 # convert MPH to meters per second
    
    dt = datetime.datetime.fromisoformat(item['timestamp'])
    vehicle.timestamp = int(dt.timestamp())
    
    # Destination is usually handled via 'trip' (trip_id) or 
    # stop_id in GTFS, but we can't map 'DOWNTOWN' directly 
    # without a static GTFS reference.    

result = feed.SerializeToString()
print(result)

# upload to S3
s3 = boto3.client('s3')
s3.put_object(
    Bucket="gocarta",
    Key="public/gtfs-rt/VehiclePositions.pb",
    Body=result,
    ContentType='application/x-protobuf',
    # Adding CacheControl prevents clients from seeing stale bus locations
    CacheControl='max-age=0, no-cache, no-store, must-revalidate'
)

# print(f"[dataops-simple-bus-stops] updated {len(results)} rows")
