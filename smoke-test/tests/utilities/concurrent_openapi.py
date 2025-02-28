import concurrent.futures
import glob
import json
import logging
import time

from deepdiff import DeepDiff

logger = logging.getLogger(__name__)


def load_tests(fixture_glob):
    """
    Scans a directory structure looking for json files which define expected tests/responses
    :param fixture_glob: Glob path such as "tests/openapi/**/*.json"
    :return: tuples of the filename and dictionary of the file content
    """
    for test_fixture in glob.glob(fixture_glob):
        with open(test_fixture) as f:
            yield (test_fixture, json.load(f))


def execute_request(auth_session, request):
    """
    Based on the request dictionary execute the request against gms
    :param auth_session: authentication
    :param request: request dictionary
    :return: output of the request
    """
    if "method" in request:
        method = request.pop("method")
    else:
        method = "post"

    url = auth_session.gms_url() + request.pop("url")

    return getattr(auth_session, method)(url, **request)


def evaluate_test(auth_session, test_name, test_data):
    """
    For each test step, execute the request and assert the expected response
    :param auth_session: authentication
    :param test_name: name of the test
    :param test_data: test steps as defined in the test file
    :return: none
    """
    try:
        assert isinstance(test_data, list), "Expected test_data is a list of test steps"
        for idx, req_resp in enumerate(test_data):
            if "description" in req_resp["request"]:
                description = req_resp["request"].pop("description")
            else:
                description = None
            if "wait" in req_resp["request"]:
                time.sleep(req_resp["request"]["wait"])
                continue
            url = req_resp["request"]["url"]
            actual_resp = execute_request(auth_session, req_resp["request"])
            diff = None
            try:
                if "response" in req_resp and "status_codes" in req_resp["response"]:
                    assert (
                        actual_resp.status_code in req_resp["response"]["status_codes"]
                    )
                else:
                    assert actual_resp.status_code in [200, 201, 202, 204]
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
                if diff:
                    logger.error(f"Unexpected diff: {diff}")
                logger.debug(f"Response content: {actual_resp.content}")
                raise e
    except Exception as e:
        logger.error(f"Error executing test: {test_name}")
        raise e


def run_tests(auth_session, fixture_globs, num_workers=3):
    """
    Given a collection of test files, run them in parallel using N workers
    :param auth_session: authentication
    :param fixture_globs: test files
    :param num_workers: concurrency
    :return: none
    """
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
