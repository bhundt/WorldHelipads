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
                                "icaoCode": entry["icaoCode"] if "icaoCode" in entry else "",
                                "name": entry["name"],
                                "operator": "Civil" if entry["type"] == 7 else "Military",
                                "elevation": entry["elevation"]["value"] if "elevation" in entry else "",
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
                                "elevation": entry["tags"]["ele"] if "ele" in entry["tags"] else "",
                            }
                        ),
                    ]
                )
    df = pd.DataFrame(data=data, columns=["lat", "lon", "source", "info_json"])
    df.to_parquet(destination_file)
    df.to_csv(destination_file.replace(".parquet", ".csv"), index=False)


def check_for_proximity(data1: pd.DataFrame, data2: pd.DataFrame, distance_threshold_m: float) -> pd.DataFrame:
    """
    Checks if the entries in data1 are within a distance threshold of the entries in data2. We return data2 with an additional column
    called 'proximity' which is True if the entry is within the distance threshold of any entry in data1.
    """
    spatial_tree = spatial_tree = BallTree(
        [(radians(coord["lat"]), radians(coord["lon"])) for _, coord in data2.iterrows()], metric="haversine"
    )

    data2["proximity"] = False

    # merge the data
    with tqdm(total=len(data1), desc="Checking for proximity") as pbar:
        for _, coord1 in data1.iterrows():
            query_point = [(radians(coord1["lat"]), radians(coord1["lon"]))]
            indices = spatial_tree.query_radius(
                query_point, r=distance_threshold_m / config.earth_radius_m, return_distance=False
            )

            for indices_for_query in indices:
                for idx in indices_for_query:
                    data2.at[idx, "proximity"] = True

            pbar.update(1)

    return data2


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
    osm_proximity = check_for_proximity(openaip_data, osm_data, max_distance_m)

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
    for _, osm_entry in osm_proximity.iterrows():
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

    # we could have duplicates in the OSM data since we keep all entries that are not matched
    df.drop_duplicates(subset=["lat", "lon"], inplace=True)

    return df


if __name__ == "__main__":
    # filter the openaip data individual jsons for the type we are interested in
    path_openaip_filtered = os.path.join(config.intermediate_folder, "openaip_filtered")
    filter_openaip_files_for_type(config.raw_data_openaip_folder, path_openaip_filtered)

    # transform the openaip data into one data file with our own schema
    openaip_heli_output_file = os.path.join(config.intermediate_folder, "openaip_transformed.parquet")
    transform_openaip_data(
        path_openaip_filtered,
        openaip_heli_output_file,
    )

    # transform the osm data into one file and our own schema
    osm_heli_output_file = os.path.join(config.intermediate_folder, "osm_heli.parquet")
    transform_osm_helipad_data(os.path.join(config.raw_data_osm_folder, "heli"), osm_heli_output_file)

    # merge the data
    merged_heli_output_file = os.path.join(config.intermediate_folder, "helipads.parquet")
    merged = merge_oaip_osm_helipads(
        pd.read_parquet(openaip_heli_output_file),
        pd.read_parquet(osm_heli_output_file),
        max_distance_m=config.radius_helipad_duplicate_m,
    )
    merged.to_parquet(merged_heli_output_file)
    merged.to_csv(merged_heli_output_file.replace(".parquet", ".csv"), index=False)
