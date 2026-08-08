# Reusable helpers for the data ingestion job.

import os
from html.parser import HTMLParser
from urllib.parse import urlparse

import requests


#
# HTML Parser for parsing the website response
# Collects href values from the directory listing HTML
#
class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []  # list to store parsed file names

    # parse the html
    # for ex : <a href="/pub/time.series/pr/pr.class">pr.class</a> line
    # calls - handle_starttag("a", [("href", "/pub/time.series/pr/pr.class")])
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.hrefs.append(value)


#
# Get website response
#
def get_website_response(contact_email, url):
    # 403 error - BLS blocks requests without a contact in the User-Agent
    headers = {"User-Agent": f"rearc-test ({contact_email})"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch {url}: {response.status_code}")
    return response


#
# Get file names from the given website url
#
def get_file_names(file_url, parser):
    # The href paths look like "/pub/time.series/pr/pr.class".
    # Strip the directory prefix so we get bare names like "pr.class".
    prefix = urlparse(file_url).path   # "/pub/time.series/pr/"
    file_names = []
    for h in parser.hrefs:
        if h.startswith(prefix) and not h.endswith("/"):
            file_names.append(h[len(prefix):])
    return file_names


#
# Return True if the file at `dest` should be (re)written, i.e. it does not
# exist yet (newly added) or its bytes differ from `content` (changed).
# Returns False when the local copy already matches, so callers can skip it.
#
def is_source_modified(dest, content):
    if not os.path.exists(dest):
        return True
    with open(dest, "rb") as f:
        return f.read() != content
