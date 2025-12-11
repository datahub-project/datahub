# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import sys

java_events = set()
with open("./metadata-service/services/src/main/java/com/linkedin/metadata/datahubusage/DataHubUsageEventType.java") as java_file:
    for line in java_file:
        if '''Event"''' not in line:
            continue
        line = line.replace("\n", "").split('"')[1]
        java_events.add(line)

ts_events = set()
with open("././datahub-web-react/src/app/analytics/event.ts") as ts_file:
    for line in ts_file:
        if '''Event,''' not in line:
            continue
        line = line.replace(",\n", "").replace(" ", "")
        ts_events.add(line)

ts_events_not_in_java = ts_events.difference(java_events)
if len(ts_events_not_in_java) > 0:
    print(f"Missing {ts_events_not_in_java} from DataHubUsageEventType.java. Please add")
    sys.exit(1)
print("Passed")
