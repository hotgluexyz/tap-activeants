"""Standalone CSV creation script."""

import os
import pandas as pd
import json
import sys
import requests
from datetime import datetime, timedelta
from collections.abc import MutableMapping

# Adding the current directory to the Python path to ensure module imports work
sys.path.append(os.path.join(os.path.dirname(__file__), 'tap_ants2'))

from client import ProductsStream, OrdersStream
from tap import TapAnts2

def flatten_dict(d, parent_key='', sep='.'):
    """Flatten a nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def fetch_order_details(order_id, token, api_url):
    """Fetch order details using the order ID."""
    try:
        url = f"{api_url}/v3/orders/{order_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get('data', {})
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
        return {}

def fetch_and_save_csv(stream_class, stream_name, output_file, tap_instance, api_url):
    """Fetch data from stream and save it to a CSV file."""
    print(f"Fetching data for stream: {stream_name}...")
    stream = stream_class(tap_instance)
    records = list(stream.get_records(context=None))

    if not records:
        print(f"No data found for stream: {stream_name}.")
        return

    print(f"Fetched {len(records)} records for stream: {stream_name}.")
    flattened_records = [flatten_dict(record) for record in records]
    df = pd.DataFrame(flattened_records)
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    else:
        print(f"Directory already exists: {output_dir}")

    df.to_csv(output_file, index=False)
    print(f"Data for stream {stream_name} saved to {output_file}")

    if stream_name == "orders":
        order_details = []
        token = tap_instance.config["token"]
        for record in records:
            order_id = record.get("id")
            if order_id:
                details = fetch_order_details(order_id, token, api_url)
                if details:
                    order_details.append(flatten_dict(details))
        if order_details:
            order_details_df = pd.DataFrame(order_details)
            order_details_output_file = "output/order_details.csv"
            order_details_df.to_csv(order_details_output_file, index=False)
            print(f"Order details saved to {order_details_output_file}")

def update_config_with_token(config_path, token, expires_in):
    """Update the config.json with the token and its expiration date."""
    with open(config_path, 'r') as f:
        config = json.load(f)

    expiration_time = datetime.now() + timedelta(seconds=expires_in)
    config['token'] = token
    config['token_expires_at'] = expiration_time.isoformat()

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=4)
    
    print("Updated config.json with the new token and its expiration date.")

def get_token(api_url, username, password):
    """Fetch a new token from the API."""
    url = f"{api_url}/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "password",
        "username": username,
        "password": password
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    token = response.json().get("access_token")
    expires_in = response.json().get("expires_in")
    return token, expires_in

def is_token_valid(expiration_time):
    """Check if the current token is still valid."""
    return datetime.fromisoformat(expiration_time) > datetime.now()

if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(__file__), '../config_tap_ants2.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    if 'token' in config and 'token_expires_at' in config and is_token_valid(config['token_expires_at']):
        print("Using existing valid token.")
    else:
        token, expires_in = get_token(config["api_url"], config["username"], config["password"])
        update_config_with_token(config_path, token, expires_in)
        config['token'] = token

    tap_instance = TapAnts2(config=config)

    # Fetch and save Products data
    fetch_and_save_csv(ProductsStream, "products", "output/products.csv", tap_instance, config["api_url"])

    # Fetch and save Orders data, along with order details
    fetch_and_save_csv(OrdersStream, "orders", "output/orders.csv", tap_instance, config["api_url"])
