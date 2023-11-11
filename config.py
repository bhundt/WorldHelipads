import os

# folders
raw_data_folder = os.path.join("data", "raw")
raw_data_openaip_folder = os.path.join(raw_data_folder, "openaip")
raw_data_osm_folder = os.path.join(raw_data_folder, "osm")
intermediate_folder = os.path.join("data", "intermediate")
export_folder = os.path.join("data", "export")

# OpenAIP data on google cloud platform
openaip_storage_bucket = "29f98e10-a489-4c82-ae5e-489dbcd4912f"
openaip_storage_postfix = "_apt.json"

# settings to query OSM data,  queries expect a bounding box replacement in the token $bbox$
osm_query_url = "https://overpass-api.de/api/interpreter"
num_lat_divisions = 18  # Divide the Earth into 18 equal latitude bands
num_lon_divisions = 36  # Divide the Earth into 36 equal longitude bands

heli_query = '[out:json];(node[aeroway~"helipad|heliport"]($bbox$);way[aeroway~"helipad|heliport"]($bbox$);relation[aeroway~"helipad|heliport"]($bbox$););out center;'
hospital_query = "[out:json];(node[amenity=hospital]($bbox$);way[amenity=hospital]($bbox$);relation[amenity=hospital]($bbox$););out center;"
offshore_query = '[out:json];(node[man_made~"offshore_platform|floating_storage"]($bbox$);way[man_made~"offshore_platform|floating_storage"]($bbox$);relation[man_made~"offshore_platform|floating_storage"]($bbox$);node["seamark:type"="platform"]($bbox$);way["seamark:type"="platform"]($bbox$);relation["seamark:type"="platform"]($bbox$););out center;'

# merge settings
radius_helipad_duplicate_m: float = 50
radius_helipad_belongs_to_hospital_m: float = 500
radius_helipad_belongs_to_offshore_m: float = 250

# physical constants
earth_radius_m = 6371000  # Earth radius in meters, approximate
