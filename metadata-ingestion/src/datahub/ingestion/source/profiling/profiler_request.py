import dataclasses


@dataclasses.dataclass
class ProfilerRequest:
    """Generic profiling request shared by GE and SQLAlchemy profilers.

    Previously lived in ge_data_profiler.py as GEProfilerRequest. Moved here
    so consumers can use it without importing the GE-heavy module.
    """

    pretty_name: str
    batch_kwargs: dict
