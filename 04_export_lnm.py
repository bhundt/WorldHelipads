import os
import re
import json

from tqdm import tqdm
import pandas as pd

import config


def extract_name(json_info: dict) -> str:
    if "name" in json_info:
        return json_info["name"]
    return ""


def extract_identifier(json_info: dict) -> str:
    if "icaoCode" in json_info:
        return json_info["icaoCode"]
    return ""


def make_pretty_description(source: str, json_info: dict) -> str:
    out = ""
    for key in json_info:
        if key == "name" or key == "icaoCode":  # already in the name / identifier
            continue
        if key == "elevation" and json_info[key] != "":
            out = out + f"Elevation: {json_info[key]}m MSL\n"
        else:
            if json_info[key] != "":
                out = out + f"{key.capitalize()}: {json_info[key]}\n"

    out = out + f"Source: {source}"

    return out


def extract_elevation_in_ft(json_info: dict) -> str:
    if "elevation" in json_info and json_info["elevation"] != "":
        # remove all non-digits and convert to ft
        digits_only = "".join(
            [
                char
                for char in str(json_info["elevation"])
                if char.isdigit() or char == "." or char == "," or char == "-"
            ]
        )
        return str(float(digits_only) * 3.28084)
    return ""


def create_longitude_boundaries():
    # First Part (Americas and Atlantic Ocean)
    western_boundary_part1 = -180
    eastern_boundary_part1 = -20

    # Second Part (Europe and Africa)
    western_boundary_part2 = -20
    eastern_boundary_part2 = 60

    # Third Part (Rest of the World)
    western_boundary_part3 = 60
    eastern_boundary_part3 = 180

    return {
        "Region 1": {"Western Boundary": western_boundary_part1, "Eastern Boundary": eastern_boundary_part1},
        "Region 2": {"Western Boundary": western_boundary_part2, "Eastern Boundary": eastern_boundary_part2},
        "Region 3": {"Western Boundary": western_boundary_part3, "Eastern Boundary": eastern_boundary_part3},
    }


def assign_region(row, boundaries):
    if boundaries["Region 1"]["Western Boundary"] <= row["Longitude"] <= boundaries["Region 1"]["Eastern Boundary"]:
        return "Region 1"
    elif boundaries["Region 2"]["Western Boundary"] <= row["Longitude"] <= boundaries["Region 2"]["Eastern Boundary"]:
        return "Region 2"
    elif boundaries["Region 3"]["Western Boundary"] <= row["Longitude"] <= boundaries["Region 3"]["Eastern Boundary"]:
        return "Region 3"
    else:
        raise ValueError(f"Could not assign region to {row['Longitude']}")


if __name__ == "__main__":
    if not os.path.exists(config.export_folder):
        os.makedirs(config.export_folder)

    df_input = pd.read_parquet(os.path.join(config.intermediate_folder, "helipads.parquet"))

    # now transform the data into the format we need for the LittleNavMap export
    intermediate_data = []
    for _, row in tqdm(df_input.iterrows(), total=len(df_input)):
        json_info = json.loads(row["info_json"])
        intermediate_data.append(
            {
                "Type": "Helipad",
                "Name": extract_name(json_info),
                "Ident": extract_identifier(json_info),
                "Latitude": row["lat"],
                "Longitude": row["lon"],
                "Elevation": extract_elevation_in_ft(json_info),
                "Magnetic Declination": "",
                "Tags": "WorldHelipads",
                "Description": make_pretty_description(row["source"], json_info),
                "Region": "",
                "Visible From": "",
                "Last Edit": "",
                "Import Filename": "",
            }
        )

    df_export = pd.DataFrame(
        columns=[
            "Type",
            "Name",
            "Ident",
            "Latitude",
            "Longitude",
            "Elevation",
            "Magnetic Declination",
            "Tags",
            "Description",
            "Region",
            "Visible From",
        ],
        data=intermediate_data,
    )

    # now assign regions to the data
    df_export["Region"] = df_export.apply(assign_region, axis=1, boundaries=create_longitude_boundaries())

    for region in df_export.Region.unique().tolist():
        df_export[df_export.Region == region].to_csv(
            os.path.join(config.export_folder, f"export_lnm_{region}.csv"), index=False, sep=","
        )
