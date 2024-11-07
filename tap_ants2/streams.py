
import requests
from singer_sdk.streams import RESTStream
from singer_sdk import typing as th  # JSON Schema typing helpers

class ActiveAntsStream(RESTStream):
    @property
    def url_base(self) -> str:
        return self.config["api_url"]
    
    @property
    def http_headers(self) -> dict:
        token = self._tap.get_token()
        headers = super().http_headers
        headers["Authorization"] = f"Bearer {token}"
        return headers

    def get_records(self, context):
        url = self.get_url(context)
        headers = self.http_headers
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get('data', [])
        return data

class ProductsStream(ActiveAntsStream):
    name = "products"
    path = "/v3/products"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("attributes", th.ObjectType(
            th.Property("sku", th.StringType),
            th.Property("status", th.StringType),
            th.Property("stockLevelType", th.StringType),
            th.Property("type", th.StringType),
            th.Property("length", th.IntegerType),
            th.Property("width", th.IntegerType),
            th.Property("height", th.IntegerType),
            th.Property("name", th.StringType),
            th.Property("hasBarcode", th.BooleanType),
            th.Property("barcode", th.StringType),
            th.Property("hasLotNumber", th.BooleanType),
            th.Property("hasSerialNumber", th.BooleanType),
            th.Property("hasExpirationDate", th.BooleanType),
            th.Property("expirationDateMargin", th.IntegerType),
            th.Property("expirationDateWarning", th.IntegerType),
            th.Property("countryOfOrigin", th.StringType),
            th.Property("hsCodes", th.ArrayType(th.ObjectType(
                th.Property("country", th.StringType),
                th.Property("hsCode", th.StringType)
            )))
        )),
        th.Property("relationships", th.ObjectType()),
        th.Property("links", th.ObjectType())
    ).to_dict()

class OrdersStream(ActiveAntsStream):
    name = "orders"
    path = "/v3/orders"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("attributes", th.ObjectType(
            th.Property("externalOrderNumber", th.StringType),
            th.Property("reference", th.StringType),
            th.Property("orderedOn", th.DateTimeType),
            th.Property("currency", th.StringType),
            th.Property("email", th.StringType),
            th.Property("preferredShippingDate", th.DateTimeType),
            th.Property("allowPartialDelivery", th.BooleanType),
            th.Property("onHold", th.BooleanType)
        )),
        th.Property("relationships", th.ObjectType()),
        th.Property("included", th.ArrayType(th.ObjectType())),
        th.Property("links", th.ObjectType())
    ).to_dict()

class OrderDetailsStream(ActiveAntsStream):
    name = "order_details"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("attributes", th.ObjectType(
            th.Property("sku", th.StringType),
            th.Property("quantity", th.IntegerType),
            th.Property("price", th.NumberType),
            th.Property("vat", th.NumberType),
            th.Property("name", th.StringType)
        )),
        th.Property("relationships", th.ObjectType()),
        th.Property("included", th.ArrayType(th.ObjectType())),
        th.Property("links", th.ObjectType())
    ).to_dict()

    def get_records(self, context):
        orders_stream = self._tap.streams["orders"]
        orders = list(orders_stream.get_records(context=context))
        records = []

        for order in orders:
            order_id = order.get("id")
            if order_id:
                url = f"{self.url_base}/v3/orders/{order_id}"
                headers = self.http_headers
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json().get('data', {})
                records.append(data)

        return records

class OrderItemsStream(ActiveAntsStream):
    name = "order_items"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("attributes", th.ObjectType(
            th.Property("sku", th.StringType),
            th.Property("quantity", th.IntegerType),
            th.Property("price", th.NumberType),
            th.Property("vat", th.NumberType),
            th.Property("name", th.StringType)
        )),
        th.Property("relationships", th.ObjectType(
            th.Property("order", th.ObjectType(
                th.Property("data", th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("type", th.StringType),
                    th.Property("links", th.ObjectType(
                        th.Property("self", th.StringType)
                    ))
                ))
            ))
        )),
        th.Property("links", th.ObjectType())
    ).to_dict()

    def get_records(self, context):
        order_details_stream = self._tap.streams["order_details"]
        order_details_records = list(order_details_stream.get_records(context=context))
        
        records = []
        for order_detail in order_details_records:
            order_items = order_detail.get("relationships", {}).get("orderItems", {}).get("data", [])
            for item in order_items:
                item_id = item.get("id")
                if item_id:
                    url = f"{self.url_base}/v3/orderitems/{item_id}"
                    headers = self.http_headers
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    data = response.json().get('data', {})
                    records.append(data)
                    
        return records
