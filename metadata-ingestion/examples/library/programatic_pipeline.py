# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.ingestion.run.pipeline import Pipeline

# The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
pipeline = Pipeline.create(
    {
        "source": {
            "type": "mysql",
            "config": {
                "username": "user",
                "password": "pass",
                "database": "db_name",
                "host_port": "localhost:3306",
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": "http://localhost:8080"},
        },
    }
)

# Run the pipeline and report the results.
pipeline.run()
pipeline.pretty_print_summary()
