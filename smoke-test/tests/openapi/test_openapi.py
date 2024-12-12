import concurrent.futures
import glob
import json
import logging
import time

from deepdiff import DeepDiff

logger = logging.getLogger(__name__)


def load_tests(fixture_glob="tests/openapi/**/*.json"):
    for test_fixture in glob.glob(fixture_glob):
        with open(test_fixture) as f:
            yield (test_fixture, json.load(f))


def execute_request(auth_session, request):
    if "method" in request:
        method = request.pop("method")
    else:
        method = "post"

    url = auth_session.gms_url() + request.pop("url")

    return getattr(auth_session, method)(url, **request)


def evaluate_test(auth_session, test_name, test_data):
    try:
        for idx, req_resp in enumerate(test_data):
            if "description" in req_resp["request"]:
                description = req_resp["request"].pop("description")
            else:
                description = None
            if "wait" in req_resp["request"]:
                time.sleep(int(req_resp["request"]["wait"]))
                continue
            url = req_resp["request"]["url"]
            actual_resp = execute_request(auth_session, req_resp["request"])
            try:
                if "response" in req_resp and "status_codes" in req_resp["response"]:
                    assert (
                        actual_resp.status_code in req_resp["response"]["status_codes"]
                    )
                else:
                    assert actual_resp.status_code in [200, 202, 204]
                if "response" in req_resp:
                    if "json" in req_resp["response"]:
                        if "exclude_regex_paths" in req_resp["response"]:
                            exclude_regex_paths = req_resp["response"][
                                "exclude_regex_paths"
                            ]
                        else:
                            exclude_regex_paths = []
                        diff = DeepDiff(
                            actual_resp.json(),
                            req_resp["response"]["json"],
                            exclude_regex_paths=exclude_regex_paths,
                            ignore_order=True,
                        )
                        assert not diff
                    else:
                        logger.warning("No expected response json found")
            except Exception as e:
                logger.error(
                    f"Error executing step: {idx}, url: {url}, test: {test_name}"
                )
                if description:
                    logger.error(f"Step {idx} Description: {description}")
                logger.error(f"Response content: {actual_resp.content}")
                raise e
    except Exception as e:
        logger.error(f"Error executing test: {test_name}")
        raise e


def run_tests(auth_session, fixture_globs, num_workers=3):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for fixture_glob in fixture_globs:
            for test_fixture, test_data in load_tests(fixture_glob=fixture_glob):
                futures.append(
                    executor.submit(
                        evaluate_test, auth_session, test_fixture, test_data
                    )
                )

        for future in concurrent.futures.as_completed(futures):
            logger.info(future.result())


def test_openapi_all(auth_session):
    run_tests(auth_session, fixture_globs=["tests/openapi/*/*.json"], num_workers=10)


# def test_openapi_v1(auth_session):
#     run_tests(auth_session, fixture_globs=["tests/openapi/v1/*.json"], num_workers=4)
#
#
# def test_openapi_v2(auth_session):
#     run_tests(auth_session, fixture_globs=["tests/openapi/v2/*.json"], num_workers=4)
#
#
# def test_openapi_v3(auth_session):
#     run_tests(auth_session, fixture_globs=["tests/openapi/v3/*.json"], num_workers=4)
