from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph


def check(gms_endpoint, gms_token):
    ASPECT_NAME = "dataPlatformInstance"
    graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=gms_token))
    search_results = graph.get_search_results()["value"]["metadata"]["aggregations"]

    platform_search_result = list(
        filter(lambda x: x["displayName"] == "Platform", search_results)
    )

    expected_total = graph.get_aspect_counts(ASPECT_NAME, "urn:li:dataset%")
    actual_total = 0

    platform_counts = {}
    if len(platform_search_result) > 0:
        platform_aggregation = platform_search_result[0]["filterValues"]
        for aggregation in platform_aggregation:
            platform_name = aggregation["entity"]
            facet_count = aggregation["facetCount"]
            actual_total += facet_count
            platform_counts[platform_name] = facet_count

            expected_facet_count = graph.get_aspect_counts(
                ASPECT_NAME, f"urn:li:dataset%{platform_name}%"
            )
            if facet_count != expected_facet_count:
                missing_percent = round(
                    (expected_facet_count - facet_count) * 100 / expected_facet_count, 2
                )
                print(
                    f"[WARN {ASPECT_NAME}] Expected to have {expected_facet_count} but found {facet_count} for {platform_name}. Missing % = {missing_percent}"
                )

    if expected_total != actual_total:
        missing_percent = round(
            (expected_total - actual_total) * 100 / expected_total, 2
        )
        print(
            f"[WARN {ASPECT_NAME}] Expected to have {expected_total} but found {actual_total}. Missing % = {missing_percent}"
        )
    else:
        print(f"[SUCCESS {ASPECT_NAME}] Expected and actual are {expected_total}")
