import datetime as dt
import json

import pandas as pd
import requests
from pydantic import BaseModel


def to_camel(string: str) -> str:
    return "".join(word.capitalize() for word in string.split("_"))


class Dataset(BaseModel):
    identifier: str
    description: str
    language: str
    title: str
    modified: dt.datetime
    release_date: dt.datetime
    modification_date: dt.datetime
    catalog: str
    version: str
    observations_modified: dt.datetime
    observation_count: int
    dataset_type: str
    _data: pd.DataFrame = None
    _metadata: dict = None

    class Config:
        alias_generator = to_camel
        underscore_attrs_are_private = True

    @classmethod
    def from_identifier(cls, identifier):
        r = requests.get(f"https://beta-odata4.cbs.nl/datasets/{identifier}")
        return cls(**json.loads(r.content))

    def __repr__(self):
        return self.description

    @property
    def df(self):
        self._ensure_have_data()
        return self._data
        
    @property
    def metadata(self):
        self._ensure_have_metadata()
        return self._metadata

    def _ensure_have_data(self):
        if self._data is None:
            target_url = f"https://beta-odata4.cbs.nl/CBS/{self.identifier}/Observations"
            self._data = self._get_odata(target_url)
    
    def _ensure_have_metadata(self):
        if self._metadata is None:
            metadata = {}
            target_url = f"https://beta-odata4.cbs.nl/CBS/{self.identifier}"

            for item in requests.get(target_url).json()['value']:
                if item['name'] not in ['Properties', 'Observations']:
                    metadata[item['name']] = requests.get(target_url + '/' + item['name']).json()
            self._metadata = metadata

    @staticmethod
    def _get_odata(target_url):
        # Method taken from:
        # https://github.com/statistiekcbs/CBS-Open-Data-v4/blob/master/Python/time_series_graph.py
        data = pd.DataFrame()
        while target_url:
            r = requests.get(target_url).json()
            data = pd.concat([data, pd.DataFrame(r["value"])])

            if "@odata.nextLink" in r:
                target_url = r["@odata.nextLink"]
            else:
                target_url = None

        return data
