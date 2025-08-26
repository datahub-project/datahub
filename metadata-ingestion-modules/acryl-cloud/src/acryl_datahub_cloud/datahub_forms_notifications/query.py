import pathlib

GRAPHQL_SCROLL_FORMS_FOR_NOTIFICATIONS = (
    pathlib.Path(__file__).parent / "scroll_forms_for_notification.gql"
).read_text()

GRAPHQL_GET_SEARCH_RESULTS_TOTAL = (
    pathlib.Path(__file__).parent / "get_search_results_total.gql"
).read_text()

GRAPHQL_SEND_FORM_NOTIFICATION_REQUEST = (
    pathlib.Path(__file__).parent / "send_form_notification_request.gql"
).read_text()

GRAPHQL_GET_FEATURE_FLAG = (
    pathlib.Path(__file__).parent / "get_feature_flag.gql"
).read_text()
