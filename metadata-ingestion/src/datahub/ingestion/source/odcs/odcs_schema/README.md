# Vendored ODCS JSON Schemas

This directory contains JSON Schema files copied unmodified from
[bitol-io/open-data-contract-standard](https://github.com/bitol-io/open-data-contract-standard)
and used to validate ODCS YAML input. Apache 2.0 — see the upstream
[LICENSE](https://github.com/bitol-io/open-data-contract-standard/blob/main/LICENSE).
The Bitol Project is part of the Linux Foundation AI & Data Foundation.

## Pinned versions

| File               | Upstream tag | Source URL                                                                                                | SHA-256                                                            |
| ------------------ | ------------ | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `odcs-v3.0.2.json` | `v3.0.2`     | <https://github.com/bitol-io/open-data-contract-standard/blob/v3.0.2/schema/odcs-json-schema-v3.0.2.json> | `fc9d774ea73d13b473a52868eb8e66784f3a031794d1d22dd94d76f8c9c07c79` |
| `odcs-v3.1.0.json` | `v3.1.0`     | <https://github.com/bitol-io/open-data-contract-standard/blob/v3.1.0/schema/odcs-json-schema-v3.1.0.json> | `2cb7dd6fe43344d2233e0406438622681dc3ebadcf8f0d606a15b40c8f6752c0` |

The authoritative checksums are tracked in `CHECKSUMS.sha256` next to
the schemas. Any change to a schema file must be accompanied by a
deliberate update to that checksum file in the same commit.

## Refreshing

Run `metadata-ingestion/scripts/refresh_odcs_schemas.sh` to update one
or more vendored copies from upstream:

```bash
# Refresh both pinned versions
metadata-ingestion/scripts/refresh_odcs_schemas.sh

# Refresh only v3.1.0
metadata-ingestion/scripts/refresh_odcs_schemas.sh v3.1.0
```

The script downloads each file from the matching upstream release tag,
recomputes the SHA-256, and verifies it against `CHECKSUMS.sha256`
**before** overwriting the vendored copy. A hash mismatch aborts with a
non-zero exit and instructs the operator to either (a) update
`CHECKSUMS.sha256` deliberately as part of the same commit, or
(b) investigate possible upstream tampering or unintended modification.

## License attribution

```
Copyright Bitol authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
