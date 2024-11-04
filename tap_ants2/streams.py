from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_activeants.client import ActiveAntsStream

class ProductsStream(ActiveAntsStream):
    """Define custom stream for products."""
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
            th.Property("countryOfOrigin", th.StringType, nullable=True),
            th.Property("hsCodes", th.ArrayType(th.ObjectType(
                th.Property("country", th.StringType),
                th.Property("hsCode", th.StringType)
            )))
        ))
    ).to_dict()

class OrdersStream(ActiveAntsStream):
    """Define custom stream for orders."""
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
        ))
    ).to_dict()
