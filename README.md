# dataops-gtfs-rt-vehicle-positions
GTFS Realtime Feed for Vehicle Positions

## download
https://gocarta.s3.us-east-2.amazonaws.com/public/gtfs-rt/VehiclePositions.pb

## background
We fuse together two datasources: vehicle locations from the Clever BusTime API and TAIP GPS messages sent from the Cradlepoint routers.  By fusing both datasets together we improve redundancy and reliability.

## frequency
The pipeline runs approximately every second on a cloud server.

## columns
| column | example | description |
| :--- | :--- | :--- |
| **vehicle_id** | `XXXX` | The unique internal identifier for the specific physical bus. |
| **route_id** | `` | Route ID |
| **trip__id** | `` | Trip ID |
| **trip_start_date** | `` | what day the trip started |
| **trip_start_time** | `` | what time the trip started |
| **direction_id** | `` | Direction ID |
| **latitude** | `` | Latitude of the Vehicle |
| **longitude | `` | Longitude of the Vehicle |
| **speed** | `` | Estimated speed from Clever Devices.  It's not reliable and should be taken with a grain of salt. |
| **bearing** | `` | Bearing of the Vehicle from Clever Device. |
| **timestamp** | `` | When the vehicle reported this location |
| **schedule_relationship | `"Scheduled"` | Whether the trip is planned or not |

## download links
- [metadata](https://gocarta.s3.us-east-2.amazonaws.com/public/data/gtfsrt_vehicle_positions/v1/meta.json)
- [csv](https://gocarta.s3.us-east-2.amazonaws.com/public/data/v/v1/data.csv)
- [geojson](https://gocarta.s3.us-east-2.amazonaws.com/public/data/gtfsrt_vehicle_positions/v1/data.points.geojson)
- [geoparquet](https://gocarta.s3.us-east-2.amazonaws.com/public/data/gtfsrt_vehicle_positions/v1/data.parquet)
- [json](https://gocarta.s3.us-east-2.amazonaws.com/public/data/gtfsrt_vehicle_positions/v1/data.json)
- [json lines](https://gocarta.s3.us-east-2.amazonaws.com/public/data/gtfsrt_vehicle_positions/v1/data.jsonl)
- [shapefile](https://gocarta.s3.us-east-2.amazonaws.com/public/data/gtfsrt_vehicle_positions/v1/data.points.shp.zip)

## preview links
- You can view the geojson on a map using [geojson.io](https://geojson.io/#data=data:text/x-url,https://gocarta.s3.us-east-2.amazonaws.com/public/data/gtfsrt_vehicle_positions/v1/data.points.geojson).
- You can view the shapefile on a map using [shapefile.io](https://shapefile.io?url=https://gocarta.s3.us-east-2.amazonaws.com/public/data/gtfsrt_vehicle_positions/v1/data.points.shp.zip).
- You can query the data with SQL using [duckdb](https://shell.duckdb.org/#queries=v0,CREATE-TABLE-dataset-AS-SELECT-*-FROM-'s3://gocarta/public/data/gtfsrt_vehicle_positions/v1/data.parquet'~,Describe-dataset~).

## support
Post an issue [here](https://github.com/gocarta/dataops-gtfs-rt-vehicle-positions/issues) or email the package author at DanielDufour@gocarta.org.
