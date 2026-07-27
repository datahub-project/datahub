from __future__ import annotations

import argparse
import logging
import sys

from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUTestRunner


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(description="ZDU upgrade lifecycle integration test")
    p.add_argument("--gms-url", default=None)
    p.add_argument("--gms-token", default=None)
    p.add_argument(
        "--skip",
        nargs="*",
        default=None,
        metavar="PHASE",
        help="Phase names to skip (override ZDU_SKIP_PHASES env)",
    )
    p.add_argument("--sweep-timeout", type=int, default=None)
    p.add_argument("--reader-workers", type=int, default=None)
    p.add_argument("--writer-workers", type=int, default=None)
    p.add_argument(
        "--only-tc",
        nargs="*",
        type=int,
        default=None,
        metavar="TC_NUMBER",
    )
    p.add_argument(
        "--suite",
        nargs="*",
        default=None,
        choices=["a", "b", "c", "d", "e", "f", "g", "h"],
        metavar="SUITE",
        help="Run only the listed suites (lowercase short codes)",
    )
    p.add_argument(
        "--no-clean-build",
        action="store_true",
        default=False,
        help=(
            "Opt OUT of nuke + redeploy in SetupOldStackPhase. Default is "
            "ON (always nuke + redeploy on OLD image before the test) so "
            "ES retains OLD-shape mappings and SystemUpdateBlocking sees "
            "real diffs. Use this for Python-only iteration against an "
            "already-prepared stack. Equivalent to ZDU_SKIP_NUKE=1."
        ),
    )
    p.add_argument(
        "--no-refresh-token",
        action="store_true",
        default=False,
        help=(
            "Opt OUT of `datahub init` token refresh after redeploy. Default "
            "is ON so every run ends up with a token that verifies against "
            "the redeployed stack's signing key. Use this when ~/.datahubenv "
            "already has a working token and you want to skip the extra ~1s. "
            "Equivalent to ZDU_SKIP_TOKEN_REFRESH=1."
        ),
    )
    args = p.parse_args()

    # Start from env, then override with explicit CLI flags only.
    config = ZDUTestConfig.from_env()
    if args.gms_url is not None:
        config.gms_url = args.gms_url
    if args.gms_token is not None:
        config.gms_token = args.gms_token
    if args.skip is not None:
        config.skip_phases = args.skip
    if args.sweep_timeout is not None:
        config.sweep_timeout_s = args.sweep_timeout
    if args.reader_workers is not None:
        config.reader_workers = args.reader_workers
    if args.writer_workers is not None:
        config.writer_workers = args.writer_workers
    if args.only_tc is not None:
        config.run_only_tc = args.only_tc
    if args.suite is not None:
        config.suites = args.suite
    if args.no_clean_build:
        config.clean_build = False
    if args.no_refresh_token:
        config.refresh_token = False

    report = ZDUTestRunner(config).run()
    print(report.summary())
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
