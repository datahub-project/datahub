from datahub.ingestion.run.pipeline import Pipeline

emitter = Pipeline.create(
    {
        "source": {
            "type": "mysql",
            "config": {
                "username": "root",
                "password": "root",
                "database": "source",
                "host_port": "localhost:3307",
                "schema_pattern": {
                    "deny": ["information_schema", "sys", "mysql", "performance_schema"]
                },
                "table_pattern": {
                    "allow": ["source.compras"]
                },
            },
        },
        "sink": {
            "type": "datahub-kafka",
            "config": {
                "connection": {
                    "bootstrap": "localhost:9092"}
            },
        },
    }
)

emitter.run()
emitter.pretty_print_summary()


