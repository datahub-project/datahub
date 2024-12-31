import os
from typing import Iterable, Tuple

import psutil

from datahub.ingestion.api.workunit import MetadataWorkUnit


def workunit_sink(workunits: Iterable[MetadataWorkUnit]) -> Tuple[int, int]:
    peak_memory_usage = psutil.Process(os.getpid()).memory_info().rss
    i: int = 0
    for i, _wu in enumerate(workunits):
        if i % 10_000 == 0:
            peak_memory_usage = max(
                peak_memory_usage, psutil.Process(os.getpid()).memory_info().rss
            )
    peak_memory_usage = max(
        peak_memory_usage, psutil.Process(os.getpid()).memory_info().rss
    )

    return i, peak_memory_usage
