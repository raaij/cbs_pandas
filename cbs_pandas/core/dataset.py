import datetime as dt
import json

import pandas as pd
import numpy as np
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
    def raw_df(self):
        self._ensure_have_data()
        result = self._data
        return result

    @property
    def df(self):
        self._ensure_have_data()
        result = self._clean_df(self._data)
        return result

    @property
    def metadata(self):
        self._ensure_have_metadata()
        return self._metadata

    def visualize(self, measure: str, aggregation = np.sum, group: str = None):
        import matplotlib.pyplot as plt
        import seaborn as sns

        df = self.df  # Make reference shorter
        assert 'Date' in df.columns, "ERROR: Must have a date column!"
        fig, ax = plt.subplots(1, 1, figsize=(15, 6))

        if not group:
            dff = df.groupby('Date').agg({measure: aggregation})
            fig = dff.plot.bar(ax=ax)
        else:
            dff = df.groupby(['Date', group]).agg({measure: aggregation}).reset_index().pivot(
                ['Date'],
                [group],
                [measure]
            )
            fig = dff.plot.bar(stacked=True, ax=ax).legend(bbox_to_anchor=(1.0, 1.0))
        return fig

    def _ensure_have_data(self):
        if self._data is None:
            target_url = f"https://beta-odata4.cbs.nl/CBS/{self.identifier}/Observations"
            self._data = self._get_odata(target_url)

    def _ensure_have_metadata(self):
        if self._metadata is None:
            metadata = {}
            target_url = f"https://beta-odata4.cbs.nl/CBS/{self.identifier}"

            for item in requests.get(target_url).json()["value"]:
                if item["name"] not in ["Properties", "Observations"]:
                    metadata[item["name"]] = requests.get(target_url + "/" + item["name"]).json()
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

    def _clean_df(self, df):
        ### Clean pipeline
        df = self._clean_drop_unecessary_column(df)
        df = self._clean_apply_metadata_mappings(df)
        df = self._clean_handle_timestamp_column(df)
        df = self._clean_make_data_pivot(df)
        df = self._clean_rename_column(df)
        df = self._clean_remove_summation_value(df)
        return df

    @staticmethod
    def _clean_drop_unecessary_column(df):
        df = df.drop(columns=["Id", "ValueAttribute"], errors="ignore")
        return df

    def _clean_apply_metadata_mappings(self, df):
        for item in [item for item in self.metadata.keys() if item.endswith("Codes")]:
            # Timestamp column, these are handled separately
            if "Perioden" in item:
                continue
            col = item.replace("Codes", "")
            for value in self.metadata[item]["value"]:
                # That what is to be replaced
                old_value = value["Identifier"]
                # That what the value is to be replaced with
                new_value = value["Title"]
                if "Unit" in value:
                    new_value += f" ({value['Unit']})"
                # Execute replace
                df[col] = df[col].replace(old_value, new_value)
        return df

    def _clean_handle_timestamp_column(self, df):
        if "Perioden" in df.columns:
            df = self._cbs_add_date_column(df)
        return df

    @staticmethod
    def _clean_make_data_pivot(df):
        df = df.pivot(
            index=[col for col in df.columns if not col in ["Measure", "Value"]],
            columns=["Measure"],
            values=["Value"],
        ).reset_index()
        return df

    @staticmethod
    def _clean_rename_column(df):
        # TODO: Clean this method up
        rename_columns = []
        for col in df.columns:
            if not col[1]:
                rename_columns.append(col[0])
            else:
                rename_columns.append(col[1])
        df.columns = rename_columns
        return df

    @staticmethod
    def _clean_remove_summation_value(df):
        df = df[~df.apply(lambda row: "Totaal" in str(row.values), axis=1)]
        return df

    @staticmethod
    def _cbs_add_date_column(data, period_name="Perioden"):
        # Method taken from:
        # https://github.com/statistiekcbs/CBS-Open-Data-v4/blob/master/Python/time_series_graph.py
        regex = r"(\d{4})([A-Z]{2})(\d{2})"
        data[["Year", "Frequency", "Count"]] = data[period_name].str.extract(regex)

        freq_dict = {"JJ": "Y", "KW": "Q", "MM": "M"}
        data = data.replace({"Frequency": freq_dict})

        # Converteert van het CBS-formaat voor perioden naar een datetime.
        def convert_cbs_period(row):
            if row["Frequency"] == "Y":
                return dt.datetime(int(row["Year"]), 1, 1)
            elif row["Frequency"] == "M":
                return dt.datetime(int(row["Year"]), int(row["Count"]), 1)
            elif row["Frequency"] == "Q":
                return dt.datetime(int(row["Year"]), int(row["Count"]) * 3 - 2, 1)
            else:
                return None

        data["Date"] = data.apply(convert_cbs_period, axis=1)
        data = data.drop(columns=["Year", "Count", "Perioden"])
        return data
