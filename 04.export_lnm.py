import os
import json

from tqdm import tqdm
import pandas as pd

import config

if __name__ == "__main__":
    if not os.path.exists(config.export_folder):
        os.makedirs(config.export_folder)

    df_input = pd.read_parquet(os.path.join(config.intermediate_folder, "helipads.parquet"))
    # df_input = df_input.iloc[1816:1817]

    intermediate_data = []
    for _, row in tqdm(df_input.iterrows(), total=len(df_input)):
        intermediate_data.append(
            {
                "Type": "Helipad",
                "Name": "",
                "Ident": "",
                "Latitude": row["lat"],
                "Longitude": row["lon"],
                "Eleveation": "",
                "Magnetic Declination": "",
                "Tags": "HeliNavData",
                "Description": f'{json.dumps(json.loads(row["info_json"]), ensure_ascii=False, indent=2)}\nSource: {row["source"]}',
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

    df_export.head(10).to_csv(os.path.join(config.export_folder, "export_lmn.csv"), index=False, sep=",")
