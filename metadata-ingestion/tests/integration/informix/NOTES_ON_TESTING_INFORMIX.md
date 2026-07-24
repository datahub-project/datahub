These integration tests run against the `icr.io/informix/informix-developer-database`
image, which is anonymously pullable from IBM's public container registry (no
login required). The image is `linux/amd64` only, so the compose file pins
`platform: linux/amd64`; on Apple Silicon this runs under emulation.

The test seeds a `testdb` database via `dbaccess` against `setup/setup.sql`,
then runs the `informix` source and diffs the output against the committed
`informix_mces_golden.json`.

Readiness is detected by probing the server directly with `onstat -` (waiting
for `On-Line` in its output) rather than the image's Docker `HEALTHCHECK`, which
flaps to `unhealthy` on slower CI runners before the multi-minute first-boot
initialization finishes. This mirrors the db2/mysql "run a readiness command in
the container" pattern.

The Informix JDBC driver (`com.ibm.informix:jdbc`) and its `org.mongodb:bson`
dependency are proprietary and are **not** vendored in this repo. On first
run, `accept_ibm_jdbc_license: true` causes the connector to download and
checksum-verify them from Maven Central (see
`src/datahub/ingestion/source/informix/driver.py`), caching them under
`~/.datahub/jars/informix`. This requires internet access.

For offline/air-gapped CI, pre-provision the two jars and pass
`driver_jar_paths: ["/path/to/jdbc-<version>.jar", "/path/to/bson-<version>.jar"]`
in the recipe instead of `accept_ibm_jdbc_license`.

`jdk4py` and `JPype1` (the JVM bridge used to talk to the JDBC driver) are
multi-arch and install fine on both x86_64 and arm64, so unlike the `db2`
integration test, this test does **not** need an `x86_64`-only `skipif` guard
for the Python side — only the Docker image itself is amd64-only.
