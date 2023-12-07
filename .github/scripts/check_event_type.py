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
