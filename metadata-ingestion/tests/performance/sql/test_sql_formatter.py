import logging

from datahub.sql_parsing.sqlglot_utils import try_format_query
from datahub.utilities.perf_timer import PerfTimer
from tests.integration.snowflake.common import large_sql_query


def run_test() -> None:
    N = 500

    with PerfTimer() as timer:
        for i in range(N):
            if i % 50 == 0:
                print(
                    f"Running iteration {i}, elapsed time: {timer.elapsed_seconds(digits=2)} seconds"
                )

            try_format_query.__wrapped__(large_sql_query, platform="snowflake")

    print(
        f"Total time taken for {N} iterations: {timer.elapsed_seconds(digits=2)} seconds"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_test()
