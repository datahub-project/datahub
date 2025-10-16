from bson import ObjectId
from deepdiff import DeepDiff

from datahub.ingestion.source.schema_inference.object import construct_schema

EXCLUDE_SCHEMA_FIELDS = [r"root\[.+\]\['types'\]", r"root\[.+\]\['delimited_name'\]"]


def test_construct_schema_basic_types():
    collection = [
        {"name": "apple", "rating": 10, "tasty": True},
        {"name": "orange", "rating": 9, "tasty": True},
        {"name": "kiwi", "rating": 8, "tasty": False},
    ]

    schema = construct_schema(collection)

    assert schema[("name",)]["type"] is str
    assert schema[("name",)]["count"] == 3
    assert not schema[("name",)]["nullable"]
    assert schema[("name",)]["delimited_name"] == "name"

    assert schema[("rating",)]["type"] is int
    assert schema[("rating",)]["count"] == 3
    assert not schema[("rating",)]["nullable"]

    assert schema[("tasty",)]["type"] is bool
    assert schema[("tasty",)]["count"] == 3
    assert not schema[("tasty",)]["nullable"]


def test_construct_schema_nullable_fields():
    collection = [
        {"name": "apple", "rating": 10},
        {"name": "orange", "rating": 9, "color": "orange"},
        {"name": "kiwi"},
    ]

    schema = construct_schema(collection)

    assert not schema[("name",)]["nullable"]
    assert schema[("rating",)]["nullable"]

    assert ("color",) in schema
    assert schema[("color",)]["nullable"]


def test_construct_schema_with_none_values():
    collection = [
        {"name": "apple", "rating": 10},
        {"name": "orange", "rating": None},
        {"name": "kiwi", "rating": 8},
    ]

    schema = construct_schema(collection)

    assert schema[("rating",)]["type"] is int
    assert schema[("rating",)]["count"] == 2
    assert schema[("rating",)]["nullable"]


def test_construct_schema_mixed_types():
    collection = [
        {"mixedType": 2},
        {"mixedType": "abc"},
        {"mixedType": {"fieldA": "a"}},
    ]

    schema = construct_schema(collection)

    assert schema[("mixedType",)]["type"] == "mixed"
    assert schema[("mixedType",)]["count"] == 3
    assert not schema[("mixedType",)]["nullable"]


def test_construct_schema_int_float_coercion():
    collection = [
        {"value": 10},
        {"value": 3.14},
        {"value": 42},
    ]

    schema = construct_schema(collection)

    assert schema[("value",)]["type"] is float
    assert schema[("value",)]["count"] == 3
    assert not schema[("value",)]["nullable"]


def test_construct_schema_nested_objects():
    collection = [
        {"user": {"name": "alice", "age": 30}},
        {"user": {"name": "bob", "age": 25}},
    ]

    schema = construct_schema(collection)

    assert schema[("user",)]["type"] is dict
    assert schema[("user",)]["count"] == 3
    assert not schema[("user",)]["nullable"]

    assert schema[("user", "name")]["type"] is str
    assert schema[("user", "name")]["count"] == 2
    assert schema[("user", "name")]["delimited_name"] == "user.name"
    assert not schema[("user", "name")]["nullable"]

    assert schema[("user", "age")]["type"] is int
    assert schema[("user", "age")]["count"] == 2
    assert schema[("user", "age")]["delimited_name"] == "user.age"
    assert not schema[("user", "age")]["nullable"]


def test_construct_schema_simple_arrays():
    collection = [
        {"varieties": ["honey crisp", "red delicious", "fuji"]},
        {"varieties": ["clementine", "navel"]},
    ]

    schema = construct_schema(collection)

    assert schema[("varieties",)]["type"] is list
    assert schema[("varieties",)]["count"] == 2
    assert not schema[("varieties",)]["nullable"]


def test_construct_schema_arrays_with_nested_objects():
    collection = [
        {
            "items": [
                {"name": "item1", "price": 10.5},
                {"name": "item2", "price": 20.0},
            ]
        },
        {
            "items": [
                {"name": "item3", "price": 15.5},
            ]
        },
    ]

    schema = construct_schema(collection)

    assert schema[("items",)]["type"] is list
    assert schema[("items",)]["count"] == 4
    assert not schema[("items",)]["nullable"]

    assert schema[("items", "name")]["type"] is str
    assert schema[("items", "name")]["count"] == 3
    assert schema[("items", "name")]["delimited_name"] == "items.name"
    assert not schema[("items", "name")]["nullable"]

    assert schema[("items", "price")]["type"] is float
    assert schema[("items", "price")]["count"] == 3
    assert schema[("items", "price")]["delimited_name"] == "items.price"
    assert not schema[("items", "price")]["nullable"]


def test_construct_schema_arrays_with_nested_objects_nullable_fields():
    collection = [
        {
            "items": [
                {"name": "item1", "price": 10.5, "discount": 0.1},
                {"name": "item2", "price": 20.0},
            ]
        },
        {
            "items": [
                {"name": "item3", "price": 15.5, "discount": 0.2},
            ]
        },
    ]

    schema = construct_schema(collection)

    assert schema[("items", "discount")]["type"] is float
    assert schema[("items", "discount")]["count"] == 2
    assert schema[("items", "discount")]["nullable"]


def test_construct_schema_empty_arrays():
    collection = [
        {"items": []},
        {"items": [{"name": "item1"}]},
    ]

    schema = construct_schema(collection)

    assert schema[("items",)]["type"] is list
    assert schema[("items",)]["count"] == 2
    assert not schema[("items",)]["nullable"]

    assert ("items", "name") in schema
    assert schema[("items", "name")]["type"] is str
    assert schema[("items", "name")]["count"] == 1
    assert schema[("items", "name")]["nullable"]


def test_construct_schema_deeply_nested_objects():
    collection = [
        {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep",
                    }
                }
            }
        },
    ]

    schema = construct_schema(collection)

    assert schema[("level1", "level2", "level3", "value")]["type"] is str
    assert schema[("level1", "level2", "level3", "value")]["count"] == 1
    assert not schema[("level1", "level2", "level3", "value")]["nullable"]
    assert (
        schema[("level1", "level2", "level3", "value")]["delimited_name"]
        == "level1.level2.level3.value"
    )


def test_construct_schema_default_delimiter():
    collection = [
        {"user": {"name": "alice", "profile": {"city": "NYC"}}},
    ]

    schema = construct_schema(collection)

    assert schema[("user", "name")]["delimited_name"] == "user.name"
    assert schema[("user", "profile", "city")]["delimited_name"] == "user.profile.city"


def test_construct_schema_custom_delimiter():
    collection = [
        {"user": {"name": "alice"}},
    ]

    schema = construct_schema(collection, delimiter="__")

    assert schema[("user", "name")]["delimited_name"] == "user__name"


def test_construct_schema_mongodb_example():
    collection = [
        {
            "_id": ObjectId("68ed590bfe328426c34f8802"),
            "name": "apple",
            "rating": 10,
            "varieties": ["honey crisp", "red delicious", "fuji"],
            "tasty": True,
            "mixedType": 2,
            "nullableMixedType": "a",
        },
        {
            "_id": ObjectId("68ed590bfe328426c34f8803"),
            "name": "orange",
            "rating": 9,
            "varieties": ["clementine", "navel"],
            "tasty": True,
            "mixedType": "abc",
            "nullableMixedType": True,
        },
        {
            "_id": ObjectId("68ed590bfe328426c34f8804"),
            "name": "kiwi",
            "rating": 1e27,
            "tasty": True,
            "mixedType": {"fieldA": "a", "fieldTwo": 2},
        },
    ]

    schema = construct_schema(collection)

    assert (
        DeepDiff(
            schema,
            {
                ("_id",): {
                    "type": ObjectId,
                    "count": 3,
                    "nullable": False,
                },
                ("name",): {
                    "type": str,
                    "count": 3,
                    "nullable": False,
                },
                ("rating",): {
                    "type": float,
                    "count": 3,
                    "nullable": False,
                },
                ("varieties",): {
                    "type": list,
                    "count": 2,
                    "nullable": True,
                },
                ("tasty",): {
                    "type": bool,
                    "count": 3,
                    "nullable": False,
                },
                ("mixedType",): {
                    "type": "mixed",
                    "count": 3,
                    "nullable": False,
                },
                ("mixedType", "fieldA"): {
                    "type": str,
                    "count": 1,
                    "nullable": True,
                },
                ("mixedType", "fieldTwo"): {
                    "type": int,
                    "count": 1,
                    "nullable": True,
                },
                ("nullableMixedType",): {
                    "type": "mixed",
                    "count": 2,
                    "nullable": True,
                },
            },
            exclude_regex_paths=EXCLUDE_SCHEMA_FIELDS,
        )
        == {}
    )


def test_construct_schema_complex_nested_arrays():
    collection = [
        {
            "products": [
                {
                    "name": "laptop",
                    "specs": {"cpu": "Intel i7", "ram": 16},
                    "reviews": [
                        {"rating": 5, "comment": "Great!"},
                        {"rating": 4, "comment": "Good"},
                    ],
                },
                {
                    "name": "mouse",
                    "specs": {"dpi": 1600},
                },
            ]
        },
        {
            "products": [
                {
                    "name": "keyboard",
                    "specs": {"switches": "mechanical"},
                    "reviews": [
                        {"rating": 5},
                    ],
                }
            ]
        },
    ]

    schema = construct_schema(collection)

    assert (
        DeepDiff(
            schema,
            {
                ("products",): {
                    "type": list,
                    "count": 5,
                    "nullable": False,
                },
                ("products", "name"): {
                    "type": str,
                    "count": 3,
                    "nullable": False,
                },
                ("products", "specs"): {
                    "type": dict,
                    "count": 4,
                    "nullable": False,
                },
                ("products", "specs", "cpu"): {
                    "type": str,
                    "count": 1,
                    "nullable": True,
                },
                ("products", "specs", "ram"): {
                    "type": int,
                    "count": 1,
                    "nullable": True,
                },
                ("products", "specs", "dpi"): {
                    "type": int,
                    "count": 1,
                    "nullable": True,
                },
                ("products", "specs", "switches"): {
                    "type": str,
                    "count": 1,
                    "nullable": True,
                },
                ("products", "reviews"): {
                    "type": list,
                    "count": 4,
                    "nullable": True,
                },
                ("products", "reviews", "rating"): {
                    "type": int,
                    "count": 3,
                    "nullable": True,
                },
                ("products", "reviews", "comment"): {
                    "type": str,
                    "count": 2,
                    "nullable": True,
                },
            },
            exclude_regex_paths=EXCLUDE_SCHEMA_FIELDS,
        )
        == {}
    )


def test_construct_schema_array_with_varying_field_counts():
    collection = [
        {
            "orders": [
                {"id": 1, "amount": 100.0, "status": "completed", "discount": 10.0},
                {"id": 2, "amount": 200.0, "status": "completed"},
                {"id": 3, "amount": 150.0, "discount": 15.0},
                {"id": 4, "amount": 250.0, "status": "pending"},
            ]
        }
    ]

    schema = construct_schema(collection)

    assert (
        DeepDiff(
            schema,
            {
                ("orders",): {
                    "type": list,
                    "count": 5,
                    "nullable": False,
                },
                ("orders", "id"): {
                    "type": int,
                    "count": 4,
                    "nullable": False,
                },
                ("orders", "amount"): {
                    "type": float,
                    "count": 4,
                    "nullable": False,
                },
                ("orders", "status"): {
                    "type": str,
                    "count": 3,
                    "nullable": True,
                },
                ("orders", "discount"): {
                    "type": float,
                    "count": 2,
                    "nullable": True,
                },
            },
            exclude_regex_paths=EXCLUDE_SCHEMA_FIELDS,
        )
        == {}
    )


def test_construct_schema_array_with_mixed_item_types():
    collection = [
        {"data": [1, "string", 3.14, {"nested": "object"}]},
    ]

    schema = construct_schema(collection)

    assert schema[("data",)]["type"] is list
    assert schema[("data",)]["count"] == 2
    assert not schema[("data",)]["nullable"]

    assert schema[("data", "nested")]["type"] is str
    assert schema[("data", "nested")]["count"] == 1
    assert not schema[("data", "nested")]["nullable"]


def test_construct_schema_empty_collection():
    collection = []

    schema = construct_schema(collection)
    assert len(schema) == 0


def test_construct_schema_empty_documents():
    collection = [{}, {}, {}]

    schema = construct_schema(collection)
    assert len(schema) == 0
