"""Ants2 tap class."""

from __future__ import annotations
import os
import pandas as pd
import requests
from singer_sdk import Tap, typing as th  # JSON schema typing helpers
from tap_ants2.client import ProductsStream, OrdersStream

class TapAnts2(Tap):
    """Ants2 tap class."""
    name = "tap-ants2"
    _token = None  # Class variable to store the token

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType),
            required=True,
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://shopapitest.activeants.nl",
        ),
    ).to_dict()

    def get_token(self):
        if not TapAnts2._token:
            url = f"{self.config['api_url']}/token"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
            }
            data = {
                "grant_type": "password",
                "username": self.config["username"],
                "password": self.config["password"]
            }

            response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            TapAnts2._token = response.json().get("access_token")
        return TapAnts2._token

    def discover_streams(self) -> list[RESTStream]:
        """Return a list of discovered streams.
        Returns:
            A list of discovered streams.
        """
        return [
            ProductsStream(self),
            OrdersStream(self),
        ]

    def post_sync(self):
        """Custom logic after sync to save CSV files."""
        self._sync_stream_to_csv("products", "output/products.csv")
        self._sync_stream_to_csv("orders", "output/orders.csv")

    def _sync_stream_to_csv(self, stream_name, output_file):
        print(f"Starting sync for stream: {stream_name} to file: {output_file}")
        streams = self.streams
        records = []
        try:
            for stream in streams.values():
                if stream.name == stream_name:
                    records = list(stream.get_records(context=None))
                    break

            if not records:
                print(f"No data found for stream: {stream_name}.")
                return

            print(f"Fetched {len(records)} records for stream: {stream_name}")

            df = pd.DataFrame(records)
            print(f"Data frame created with {df.shape[0]} rows and {df.shape[1]} columns.")

            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created directory: {output_dir}")
            else:
                print(f"Directory already exists: {output_dir}")

            print(f"Saving data to {output_file}...")
            df.to_csv(output_file, index=False)
            print(f"Data for stream {stream_name} saved to {output_file}")

        except Exception as e:
            print(f"An error occurred during syncing: {e}")

if __name__ == "__main__":
    tap = TapAnts2()
    tap.run_sync()
    tap.post_sync()
