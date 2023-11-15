World Helipads
---
This project contains a couple of scripts to extract helipads from [OpenStreetMap](https://www.openstreetmap.org/#map=19/46.58327/8.02259) and [OpenAIP](https://www.openaip.net) and convert them to a format that can be used in other tools (for example [LittleNavMap](https://albar965.github.io/littlenavmap.html)).

Thanks to the OpenStreetMap contributors and OpenAIP (under Attribution-NonCommercial 4.0 International license).

Data is extracted from OpenStreetMap using [Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API) and from OpenAIP using [OpenAIP Data Exports](https://www.openaip.net/docs).

## Usage
This project uses Python and [Poetry](https://python-poetry.org) to manage dependencies and virtual environments. To install dependencies, run:
```bash 
poetry install
```

To retrieve the data, run:
```bash
poetry run python 01_retrieve_data.py
```

To convert the data into an intermediate format and perform some filtering and deduplication, run:
```bash
poetry run python 02_filter_and_merge.py
```

To convert the data to a format that can be used in LittleNavMap, run:
```bash
poetry run python 04_export_lnm.py
```
Your will find the data in the `data/export` folder split into three regions. You can import the files as userpoints in LittleNavMap by using the `Userpoints -> Import CSV ... ` menu.