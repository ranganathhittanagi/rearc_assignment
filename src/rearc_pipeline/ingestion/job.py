# Data ingestion script
# Config the sources

import json
import os
import sys
import inspect
from urllib.parse import urljoin
import logging

# Allow importing the local package when Databricks runs this file directly.
# Serverless runs the file via exec(), so __file__ is not defined; use the
# code object's filename instead.
_THIS_FILE = inspect.currentframe().f_code.co_filename
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(_THIS_FILE), "..", "..")))

from rearc_pipeline.ingestion.utils import (
    LinkParser,
    get_website_response,
    get_file_names,
    is_source_modified,
)


import argparse

# Parameters (overridable when run as a Databricks job).
def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume_path", default="/Volumes/workspace/default/rearc_raw")  # A Unity Catalog Volume path.
    parser.add_argument("--contact_email", default="ranganath.hittanagi@gmail.com")
    return parser.parse_args()

args = _get_args()
VOLUME_PATH = args.volume_path
CONTACT_EMAIL = args.contact_email

SOURCES = [
    {
        "source_type": "website",
        "source_url": "https://download.bls.gov/pub/time.series/pr/",
        "target_path": "pr/"
    },
    {
        "source_type": "api",
        "source_url": (
            "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
            "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation"
            "&locale=en&measures=Population"
        ),
        "target_path": "population.json"
    },
]

def api_ingestion(source_url, target_path):
    r = get_website_response(CONTACT_EMAIL, source_url)
    payload = r.json()
    # Save the raw response into the Volume, but only if it changed.
    dest = os.path.join(VOLUME_PATH, target_path)
    content = json.dumps(payload).encode()
    if is_source_modified(dest, content):
        with open(dest, "wb") as f:
            f.write(content)
        logging.info(f"saved {dest}")
    else:
        logging.info(f"skip {dest} (unchanged)")


def website_ingestion(source_url, target_path):
    # 1. Fetch the directory listing page and parse out the file names.
    response = get_website_response(CONTACT_EMAIL, source_url)
    parser = LinkParser()
    parser.feed(response.text)
    file_names = get_file_names(source_url, parser)

    # 2. Make sure the destination folder exists inside the Volume.
    dest_dir = os.path.join(VOLUME_PATH, target_path)
    os.makedirs(dest_dir, exist_ok=True)

    # 3. Download each file and write it into the Volume only if it is new or
    #    its bytes changed, so re-runs don't reprocess unchanged files.
    for name in file_names:
        file_url = urljoin(source_url, name)
        # ex : file_url = https://download.bls.gov/pub/time.series/pr/pr.data.0.Current
        file_response = get_website_response(CONTACT_EMAIL, file_url)
        dest = os.path.join(dest_dir, name)
        if is_source_modified(dest, file_response.content):
            with open(dest, "wb") as f:             # "wb" = write bytes
                f.write(file_response.content)
            logging.info(f"saved {name} ({len(file_response.content)} bytes)")
        else:
            logging.info(f"skip {name} (unchanged)")

    # 4. Remove local files that no longer exist at the source.
    for name in os.listdir(dest_dir):
        if name not in file_names:
            os.remove(os.path.join(dest_dir, name))
            logging.info(f"removed {name} (no longer at source)")

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    logging.info(f"Saving data to {VOLUME_PATH}")

    # Dispatch each configured source to its handler.
    os.makedirs(VOLUME_PATH, exist_ok=True)

    for source in SOURCES:
        source_type = source["source_type"]
        source_url = source["source_url"]
        target_path = source["target_path"]

        if source_type == "api":
            api_ingestion(source_url, target_path)
        elif source_type == "website":
            website_ingestion(source_url, target_path)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")

    logging.info(f"Data saved to {VOLUME_PATH}")
