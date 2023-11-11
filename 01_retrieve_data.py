import os
import json

import requests
from tqdm import tqdm
from google.cloud import storage
from time import sleep

import config


def retrieve_file_from_google_storage_anonymous(
    bucket_name: str, file_name: str, destination_file_name: str = None
) -> None:
    """
    Retrieve file from google storage and save it to local disk
    :param bucket_name: name of the bucket
    :param file_name: name of the file to be saved
    :param destination_file_name: name of the file to be saved
    :return: None
    """
    storage_client = storage.Client.create_anonymous_client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(destination_file_name)


def retrieve_google_storage_file_list(
    bucket_name: str, prefix: str = None, postfix: str = None
) -> list:
    """
    Retrieve list of files from google storage and filter it by prefix
    :param bucket_name: name of the bucket
    :param prefix: prefix to filter the list of files
    :param postfix: postfix to filter the list of files
    :return: list of file names on google storage
    """
    storage_client = storage.Client.create_anonymous_client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    matches = []
    for blob in blobs:
        if prefix is not None and not blob.name.startswith(prefix):
            continue
        if postfix is not None and not blob.name.endswith(postfix):
            continue
        matches.append(blob.name)

    return matches


def load_openaip_data(
    bucket_name: str,
    data_folder: str,
    postfix: str = None,
) -> list:
    """
    Load OpenAIP data from google storage
    :param bucket_name: name of the bucket
    :param data_folder: folder to save the data to
    :param postfix: postfix to filter the list of files
    :return: list of file names on google storage
    """
    oaip_files = retrieve_google_storage_file_list(
        bucket_name=bucket_name, postfix=postfix
    )

    for file in tqdm(
        oaip_files, desc="Downloading OpenAIP Data", total=len(oaip_files), unit="files"
    ):
        if os.path.exists(os.path.join(data_folder, file)):
            continue
        retrieve_file_from_google_storage_anonymous(
            bucket_name=bucket_name,
            file_name=file,
            destination_file_name=os.path.join(data_folder, file),
        )


def execute_osm_query_for_bbox(
    query: str, lat_min: float, lon_min: float, lat_max: float, lon_max: float
) -> dict:
    """
    Retrieve OSM data from Overpass API for a given bounding box
    :param lat_min: minimum latitude
    :param lon_min: minimum longitude
    :param lat_max: maximum latitude
    :param lon_max: maximum longitude
    :return: None
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    payload = {
        "data": query.replace("$bbox$", f"{lat_min}, {lon_min}, {lat_max}, {lon_max}")
    }

    response = requests.post(config.osm_query_url, data=payload, headers=headers)

    if response.status_code != 200:
        print(
            f"Error: Unable to retrieve data from the Overpass API. Status code: {response.status_code}. \n\nResponse: {response.text}",
        )
        return None

    return response.json()


def make_world_bounding_boxes():
    """
    Creates a list of bounding boxes that cover the entire world, divided into
    `config.num_lat_divisions` latitude divisions and `config.num_lon_divisions`
    longitude divisions.

    Returns:
        A list of tuples, where each tuple represents a bounding box and contains
        four values: the starting latitude, starting longitude, ending latitude,
        and ending longitude of the bounding box.
    """
    bounding_boxes = []
    for lat_div in range(config.num_lat_divisions):
        for lon_div in range(config.num_lon_divisions):
            # Calculate latitude and longitude ranges for the bounding box
            lat_start = -90 + (lat_div * (180 / config.num_lat_divisions))
            lat_end = -90 + ((lat_div + 1) * (180 / config.num_lat_divisions))
            lon_start = -180 + (lon_div * (360 / config.num_lon_divisions))
            lon_end = -180 + ((lon_div + 1) * (360 / config.num_lon_divisions))

            # Create bounding box coordinates
            bounding_box = (lat_start, lon_start, lat_end, lon_end)

            # Use the bounding box for your specific purpose (e.g., querying data within the box)
            bounding_boxes.append(bounding_box)
    return bounding_boxes


def load_osm_data(data_folder: str):
    """
    Downloads OSM data for helipads and hospitals in the world and saves it to a local folder.

    Args:
        data_folder (str): The path to the folder where the downloaded data will be saved.

    Returns:
        None
    """
    # function code here


def load_osm_data(data_folder: str):
    bounding_boxes = make_world_bounding_boxes()

    def download_data_for_bbox(bbox, data_folder, query, data_type) -> bool:
        folder = os.path.join(data_folder, data_type)
        if not os.path.exists(folder):
            os.makedirs(folder)

        filename = f"{bbox}.json"
        if os.path.exists(os.path.join(folder, filename)):
            return False

        # Retrieve data for the bounding box
        data = execute_osm_query_for_bbox(query, *bbox)

        # Save the data to a file
        with open(os.path.join(folder, filename), "w") as f:
            json.dump(data, f)

        return True

    for bbox in tqdm(
        bounding_boxes,
        desc="Downloading OSM Helipad Data",
        total=len(bounding_boxes),
        unit="files",
    ):
        if (
            download_data_for_bbox(bbox, data_folder, config.heli_query, "heli")
            == False
        ):
            continue

        # Sleep to avoid overloading the server
        sleep(0.5)

    for bbox in tqdm(
        bounding_boxes,
        desc="Downloading OSM Hospital Data",
        total=len(bounding_boxes),
        unit="files",
    ):
        if (
            download_data_for_bbox(bbox, data_folder, config.hospital_query, "hospital")
            == False
        ):
            continue

        # Sleep to avoid overloading the server
        sleep(0.5)

    for bbox in tqdm(
        bounding_boxes,
        desc="Downloading OSM Offshore Data",
        total=len(bounding_boxes),
        unit="files",
    ):
        if (
            download_data_for_bbox(bbox, data_folder, config.offshore_query, "offshore")
            == False
        ):
            continue

        # Sleep to avoid overloading the server
        sleep(0.5)


if __name__ == "__main__":
    if not os.path.exists(config.raw_data_openaip_folder):
        os.makedirs(config.raw_data_openaip_folder)
    if not os.path.exists(config.raw_data_osm_folder):
        os.makedirs(config.raw_data_osm_folder)

    load_openaip_data(
        bucket_name=config.openaip_storage_bucket,
        data_folder=config.raw_data_openaip_folder,
        postfix=config.openaip_storage_postfix,
    )

    load_osm_data(data_folder=config.raw_data_osm_folder)
