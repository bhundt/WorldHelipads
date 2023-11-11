import os
import json
from math import radians, cos, sin, atan2, sqrt
from multiprocessing import Pool, cpu_count

from tqdm import tqdm
import pandas as pd
from sklearn.neighbors import BallTree

import config


def filter_openaip_files_for_type(source_directory: str, destination_directory: str) -> None:
    """
    Filters OpenAIP files in the specified directory for entries of type 7 ("Heliport civil") or 4 ("Heliport Military), and saves the filtered data to a new file
    in a subdirectory called 'filtered'.

    Args:
        directory (str): The path to the directory containing the OpenAIP files to filter.

    Returns:
        None
    """
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)
    for filename in tqdm(
        os.listdir(source_directory),
        desc="Filtering OpenAIP Data",
        total=len(os.listdir(source_directory)),
        unit="files",
    ):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(source_directory, filename), mode="rb") as file:
            data = json.load(file)

            out_data = []
            for entry in data:
                if entry["type"] == 7 or entry["type"] == 4:
                    out_data.append(entry)

            with open(os.path.join(destination_directory, filename), "w+") as filtered_file:
                filtered_file.write(json.dumps(out_data, ensure_ascii=False))


def transform_openaip_data(source_directory: str, destination_file: str) -> None:
    """
    Transforms OpenAIP data from the specified directory into a CSV file containing the following columns:
    - lat (float): The latitude of the helipad.
    - lon (float): The longitude of the helipad.
    - source (str): The source of the data.
    - info_json (str): Additional data in JSON representation.
    """
    data = []
    for filename in tqdm(
        os.listdir(source_directory),
        desc="Transforming OpenAIP Data",
        total=len(os.listdir(source_directory)),
        unit="files",
    ):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(source_directory, filename)) as file:
            json_data = json.loads(file.read())
            for entry in json_data:
                data.append(
                    [
                        entry["geometry"]["coordinates"][1],
                        entry["geometry"]["coordinates"][0],
                        "OpenAIP",
                        json.dumps(
                            {
                                "icaoCode:": entry["icaoCode"] if "icaoCode" in entry else "",
                                "name": entry["name"],
                                "operator": "Civil" if entry["type"] == 7 else "Military",
                            }
                        ),
                    ]
                )
    df = pd.DataFrame(data=data, columns=["lat", "lon", "source", "info_json"])
    df.to_parquet(destination_file)
    df.to_csv(destination_file.replace(".parquet", ".csv"), index=False)


def transform_osm_helipad_data(source_directory: str, destination_file: str) -> None:
    """
    Transforms OpenAIP data from the specified directory into a CSV file containing the following columns:
    - lat (float): The latitude of the helipad.
    - lon (float): The longitude of the helipad.
    - source (str): The source of the data.
    - info_json (str): Additional data in JSON representation.
    """
    data = []
    for filename in tqdm(
        os.listdir(source_directory),
        desc="Transforming OSM Data",
        total=len(os.listdir(source_directory)),
        unit="files",
    ):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(source_directory, filename)) as file:
            json_data = json.loads(file.read())
            for entry in json_data["elements"]:
                lat = None
                lon = None
                if entry["type"] == "node":
                    lat = entry["lat"]
                    lon = entry["lon"]
                elif entry["type"] == "way" or entry["type"] == "relation":
                    lat = entry["center"]["lat"]
                    lon = entry["center"]["lon"]

                data.append(
                    [
                        lat,
                        lon,
                        "OSM",
                        json.dumps(
                            {
                                "name": entry["tags"]["name"] if "name" in entry["tags"] else "",
                                "icaoCode": entry["tags"]["icao"] if "icao" in entry["tags"] else "",
                                "surface": entry["tags"]["surface"] if "surface" in entry["tags"] else "",
                                "operator": entry["tags"]["operator:type"] if "operator:type" in entry["tags"] else "",
                                "description": entry["tags"]["description"] if "description" in entry["tags"] else "",
                            }
                        ),
                    ]
                )
    df = pd.DataFrame(data=data, columns=["lat", "lon", "source", "info_json"])
    df.to_parquet(destination_file)
    df.to_csv(destination_file.replace(".parquet", ".csv"), index=False)


def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> None:
    """
    Calculate the distance between two points on the Earth's surface using the Haversine formula.

    Args:
        lat1 (float): The latitude of the first point in degrees.
        lon1 (float): The longitude of the first point in degrees.
        lat2 (float): The latitude of the second point in degrees.
        lon2 (float): The longitude of the second point in degrees.

    Returns:
        distance (float): The distance between the two points in meters.
    """
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = radians(lat1), radians(lon1), radians(lat2), radians(lon2)

    # Haversine formula
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = c * config.earth_radius_m

    return distance


def check_for_proximity(data1: pd.DataFrame, data2: pd.DataFrame, distance_threshold_m: float) -> pd.DataFrame:
    """
    Checks if the entries in data1 are within a distance threshold of the entries in data2. We return data2 with an additional column
    called 'proximity' which is True if the entry is within the distance threshold of any entry in data1.
    """
    spatial_tree = spatial_tree = BallTree(
        [(radians(coord["lat"]), radians(coord["lon"])) for _, coord in data2.iterrows()], metric="haversine"
    )

    for data1_entry_idx, _ in data1.iterrows():
        data1.at[data1_entry_idx, "proximity"] = False

    for data2_entry_idx, _ in data2.iterrows():
        data2.at[data2_entry_idx, "proximity"] = False

    # merge the data
    with tqdm(total=len(data1), desc="Checking for proximity") as pbar:
        for data1_entry_idx, coord1 in data1.iterrows():
            query_point = [(radians(coord1["lat"]), radians(coord1["lon"]))]
            indices = spatial_tree.query_radius(
                query_point, r=distance_threshold_m / config.earth_radius_m, return_distance=False
            )

            for indices_for_query in indices:
                for idx in indices_for_query:
                    data2.at[idx, "proximity"] = True

            pbar.update(1)

    return data2


def _process_chunks_spatial_data(chunk) -> list:
    """
    Processes a chunk of spatial data by merging OpenAIP and OSM data based on a distance threshold.

    Args:
        chunk: A tuple containing the OpenAIP dataframe, OSM dataframe, distance threshold in meters, and a spatial tree.

    Returns:
        A list of merged data containing latitude, longitude, source, and info_json.

    """
    openaip_df, osm_df, distance_threshold_m = chunk

    spatial_tree = spatial_tree = BallTree(
        [(radians(coord["lat"]), radians(coord["lon"])) for _, coord in osm_df.iterrows()], metric="haversine"
    )

    for openaip_entry_idx, _ in openaip_df.iterrows():
        openaip_df.at[openaip_entry_idx, "keep"] = True  # we always keep the openaip entry

    for osm_entry_idx, _ in osm_df.iterrows():
        osm_df.at[osm_entry_idx, "keep"] = True  # we keep evertything, only remove if we find a match in the next step

    # merge the data
    with tqdm(total=len(openaip_df), desc="Merging OpenAIP & OSM Data") as pbar:
        for openaip_entry_idx, coord1 in openaip_df.iterrows():
            query_point = [(radians(coord1["lat"]), radians(coord1["lon"]))]
            indices = spatial_tree.query_radius(
                query_point, r=distance_threshold_m / config.earth_radius_m, return_distance=False
            )

            for indices_for_query in indices:
                for idx in indices_for_query:
                    osm_df.at[idx, "keep"] = False

            pbar.update(1)

    # osm_df = check_for_proximity(openaip_df, osm_df, distance_threshold_m)

    # combine the data, we always keep the openaip entry
    data = []
    for _, openaip_entry in openaip_df.iterrows():
        data.append(
            [
                openaip_entry["lat"],
                openaip_entry["lon"],
                openaip_entry["source"],
                openaip_entry["info_json"],
            ]
        )
    for _, osm_entry in osm_df.iterrows():
        if osm_entry["keep"]:
            data.append(
                [
                    osm_entry["lat"],
                    osm_entry["lon"],
                    osm_entry["source"],
                    osm_entry["info_json"],
                ]
            )
    return data


def merge_oaip_osm_helipads(
    openaip_data: pd.DataFrame, osm_data: pd.DataFrame, max_distance_m: float = 25
) -> pd.DataFrame:
    """
    Merges the OpenAIP and OSM data using the Haversine formula to calculate the distance between the helipads.

    Args:
        openaip_data (DataFrame): The path to the OpenAIP data.
        osm_data (DataFrame): The path to the OSM data.
        max_distance_m (float): The maximum distance in kilometers between the helipads to be considered a match.

    Returns:
        None
    """
    # openaip_data["keep"] = False
    # osm_data["keep"] = False

    # data = _process_chunks_spatial_data((openaip_data, osm_data, max_distance_m))

    osm_df = check_for_proximity(openaip_data, osm_data, max_distance_m)

    # combine the data, we always keep the openaip entry
    data = []
    for _, openaip_entry in openaip_data.iterrows():
        data.append(
            [
                openaip_entry["lat"],
                openaip_entry["lon"],
                openaip_entry["source"],
                openaip_entry["info_json"],
            ]
        )
    for _, osm_entry in osm_df.iterrows():
        if not osm_entry["proximity"]:
            data.append(
                [
                    osm_entry["lat"],
                    osm_entry["lon"],
                    osm_entry["source"],
                    osm_entry["info_json"],
                ]
            )

    # save the data
    df = pd.DataFrame(
        data=data,
        columns=[
            "lat",
            "lon",
            "source",
            "info_json",
        ],
    )

    # we will have duplicates in the OSM data since we keep all entries that are not matched
    df.drop_duplicates(subset=["lat", "lon"], inplace=True)

    return df


def reduce_dataset_size(source_file: str, destination_file: str, max_entries: int) -> None:
    """
    Reduces the size of the dataset by randomly removing entries.

    Args:
        source_file (str): The path to the source file.
        max_entries (int): The maximum number of entries to keep.

    Returns:
        None
    """
    df = pd.read_parquet(source_file)
    df = df.sample(n=max_entries, random_state=42)
    df.to_parquet(destination_file)
    df.to_csv(destination_file.replace(".parquet", ".csv"), index=False)


if __name__ == "__main__":
    # filter_openaip_files_for_type("data/raw/openaip", "data/intermediate/openaip_filtered/")
    path_openaip_filtered = os.path.join(config.intermediate_folder, "openaip_filtered")
    filter_openaip_files_for_type(config.raw_data_openaip_folder, path_openaip_filtered)

    openaip_heli_output_file = os.path.join(config.intermediate_folder, "openaip_transformed.parquet")
    transform_openaip_data(
        path_openaip_filtered,
        openaip_heli_output_file,
    )

    osm_heli_outpur_file = os.path.join(config.intermediate_folder, "osm_heli.parquet")
    transform_osm_helipad_data(os.path.join(config.raw_data_osm_folder, "heli"), osm_heli_outpur_file)

    merged_heli_output_file = os.path.join(config.intermediate_folder, "helipads.parquet")
    merged = merge_oaip_osm_helipads(
        pd.read_parquet(openaip_heli_output_file),
        pd.read_parquet(osm_heli_outpur_file),
        max_distance_m=config.radius_helipad_duplicate_m,
    )
    merged.to_parquet(merged_heli_output_file)
    merged.to_csv(merged_heli_output_file.replace(".parquet", ".csv"), index=False)
