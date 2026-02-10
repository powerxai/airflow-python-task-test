from ..EventDetectionModel import EventDetectionModel
import pandas as pd
import numpy as np

from utilities.models.TimeSeriesModels import InstantiatedValueType

from utilities.helpers import *


class BatteryCapacityChange(EventDetectionModel):
    name = "Battery Capacity Change"
    entity_type: str = "battery"
    run_frequency = 24
    data_window_hours = 25
    timestamp_value: str = "5T"
    event_ids = [100056]
    required_metrics = {
        f"BATTERY_SOH": InstantiatedValueType(
            "BATTERY_SOH",
            "avg",
            "00:05:00",
        )
    }
    required_config = ["has_battery"]

    def detect(self) -> list:
        if (
            self.site_config["has_battery"] == None
            or self.site_config["has_battery"] == False
        ):
            return []

        data = self.check_soh_columns(self.data)
        if len([col for col in data.columns if "SOH" in col]) == 0:
            return []
        data = self.tag_battery_capacity_change(data)
        event_list = self.create_event_list(data)

        event_list = consolidate_current_and_previous_events(
            event_list,
            self.previous_event_list,
            100056,
            {"minCapacityReached": "min", "maxNumberBatteriesCapacityChanged": "max"},
            timedelta_minutes=0,
            time_difference_type="Minutes",
        )

        return event_list

    def check_soh_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Removes cols that we cannot determine if they should be populated, and adds some processing of SOH cols"""
        cols_to_keep = []
        for col in data.columns:
            if col == "TIMESTAMP":
                cols_to_keep.append(col)
                continue
            if "SOH" not in col:
                continue
            if data[col].isnull().all():
                continue
            if data[col].max() > 2:
                data[col] = data[col] / 100
            data[col] = np.where(data[col] > 1, 1, data[col])
            data[col] = np.where(data[col] < 0, 0, data[col])
            if data[col].sum() == 0:
                continue
            else:
                cols_to_keep.append(col)

        data = data[cols_to_keep]

        return data

    def tag_battery_capacity_change(self, data: pd.DataFrame) -> pd.DataFrame:
        """Creates columns populated with number and max temperature of batteries if they exceed 45 degrees"""
        data = data.fillna(method="ffill", limit=5).fillna(method="bfill", limit=1)
        data = data.apply(self.tag_event_rows, axis=1)
        return data

    def tag_event_rows(self, row: pd.Series) -> pd.Series:
        """
        Returns 1 if any vals are less than 0.2. At the moment we don't have battery numbers and missing batteries will be
        populated with 0, so for now we exclude 0 values
        """
        v = []
        for col in row.index:
            if col == "TIMESTAMP":
                continue
            v.append(row[col])
        row["MIN_CAPACITY"] = min(v) if any(x < 0.2 for x in v) else 1
        row["NUMBER_BATTERIES"] = (
            len([x for x in v if x < 0.2]) if any(x < 0.2 for x in v) else 0
        )
        return row

    def create_event_list(self, data: pd.DataFrame) -> list:
        """Loops through instances where consecutive rows exceed 45 degrees"""
        event_list = []
        df_filt = group_data(data, "NUMBER_BATTERIES", 0, direction="above")
        for k, v in df_filt:
            event_list = self.add_event_to_list(v, event_list)
        return event_list

    def add_event_to_list(self, df_event: pd.DataFrame, event_list: list) -> list:
        """Creates event dict from event dataframe and adds to list"""

        stop_date = pd.to_datetime(df_event["TIMESTAMP"].max()) + timedelta(minutes=5)

        event_list.append(
            {
                "startDateTime": (
                    pd.to_datetime(df_event["TIMESTAMP"].min(), utc=True)
                ).isoformat(),
                "stopDateTime": stop_date.isoformat(),
                "eventTypeId": 100056,
                "entityId": self.entity_id,
                "severityCode": "High",
                "details": {
                    "minCapacityReached": df_event["MIN_CAPACITY"].min(),
                    "maxNumberBatteriesCapacityChanged": df_event[
                        "NUMBER_BATTERIES"
                    ].max(),
                    "lengthOfEventInMinutes": compute_time_difference(
                        df_event["TIMESTAMP"].min(),
                        stop_date,
                        difference_type="Minutes",
                    ),
                    "version": 1,
                },
            }
        )
        return event_list
