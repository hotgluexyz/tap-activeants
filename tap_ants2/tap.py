"""Ants2 tap class."""

from __future__ import annotations
import os
import pandas as pd
import requests
import json
from singer_sdk import Tap, typing as th  # JSON schema typing helpers
from tap_ants2.client import ProductsStream, OrdersStream, OrderDetailsStream

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
            OrderDetailsStream(self),
        ]

    def transform_record(self, record, schema):
        """Transform record to match the schema."""
        flattened_record = {}
        for field in schema:
            keys = field.split(".")
            value = record
            for key in keys:
                value = value.get(key) if value else None
            flattened_record[field] = value
        return flattened_record

    def sync_all_to_csv(self):
        self._sync_stream_to_csv("products", "output/products.csv", self.PRODUCTS_SCHEMA)
        self._sync_stream_to_csv("orders", "output/orders.csv", self.ORDERS_SCHEMA)
        self._sync_order_details_to_csv()

    def _sync_stream_to_csv(self, stream_name, output_file, schema):
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
            transformed_records = [self.transform_record(record, schema) for record in records]

            df = pd.DataFrame(transformed_records)
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

    def _sync_order_details_to_csv(self):
        print("Starting sync for order details to file: output/order_details.csv")
        order_details = []
        token = self.config["token"]
        api_url = self.config["api_url"]
        orders_stream = self.streams["orders"]
        orders = list(orders_stream.get_records(context=None))

        for order in orders:
            order_id = order.get("id")
            if order_id:
                details = self._fetch_order_details(order_id, token, api_url)
                order_details.append(self.transform_record(details, self.ORDER_DETAILS_SCHEMA))

        df = pd.DataFrame(order_details)
        output_dir = "output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")
        else:
            print(f"Directory already exists: {output_dir}")

        df.to_csv("output/order_details.csv", index=False)
        print("Data for order details saved to output/order_details.csv")

    def _fetch_order_details(self, order_id, token, api_url):
        """Fetch order details using the order ID."""
        url = f"{api_url}/v3/orders/{order_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get('data', {})

if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config_tap_ants2.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    tap_instance = TapAnts2(config=config)
    tap_instance.run_sync()
    tap_instance.sync_all_to_csv()

