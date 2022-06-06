import contextlib
import json
import os

import requests
from finder import Finder
from IPython.core.display import HTML, display

from cbs_pandas.core.dataset import Dataset

SEARCH_RESULT_MAX_HITS = 5
SEARCH_RESULT_MAX_LENGTH_DESCRIPTION = 250


class Client:
    def __init__(self):
        self._db = self._build_db()
        self._finder = self._build_finder()

    def _build_db(self):
        r = requests.get("https://beta-odata4.cbs.nl/Datasets")

        return [
            {
                "identifier": item["Identifier"],
                "title": item["Title"],
                "description": item["Description"],
            }
            for item in json.loads(r.content)["value"]
        ]

    def _build_finder(self):
        with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
            finder = Finder(self._db)
        return finder

    def search(self, keyword):
        """
        TODO:
            - Pretty printing of output should be optional.
        """
        hits = self._finder.find(keyword)
        for hit in hits[:SEARCH_RESULT_MAX_HITS]:
            identifier = hit.value["identifier"]
            title = hit.value["title"]
            short_description = hit.value["description"]

            dataset_link = f"<a href=https://opendata.cbs.nl/statline/#/CBS/nl/dataset/{identifier}>{identifier}</a>"

            if len(short_description) > SEARCH_RESULT_MAX_LENGTH_DESCRIPTION:
                short_description = short_description[:SEARCH_RESULT_MAX_LENGTH_DESCRIPTION] + "..."
            display(
                HTML(
                    f"""<h4>{title} ({dataset_link})</h4>
<p>{short_description}</p>"""
                )
            )

    def get(self, dataset):
        return Dataset.from_identifier(dataset)
