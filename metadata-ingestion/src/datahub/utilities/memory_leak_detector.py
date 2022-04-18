import fnmatch
import gc
import logging
import sys
import tracemalloc
from collections import defaultdict
from functools import wraps
from typing import Any, Callable, Dict, List, TypeVar, Union, cast

logger = logging.getLogger(__name__)
T = TypeVar("T")


def _trace_has_file(trace: tracemalloc.Traceback, file_pattern: str) -> bool:
    for frame_index in range(0, len(trace)):
        cur_frame = trace[frame_index]
        if fnmatch.fnmatch(cur_frame.filename, file_pattern):
            return True
    return False


def _init_leak_detection() -> None:
    # Initialize trace malloc to track up to 25 stack frames.
    tracemalloc.start(25)
    if sys.version_info >= (3, 9):
        # Nice to reset peak to 0. Available for versions >= 3.9.
        tracemalloc.reset_peak()
    # Enable leak debugging in the garbage collector.
    gc.set_debug(gc.DEBUG_LEAK)


def _perform_leak_detection() -> None:
    # Log potentially useful memory usage metrics
    logger.info(f"GC count before collect {gc.get_count()}")
    traced_memory_size, traced_memory_peak = tracemalloc.get_traced_memory()
    logger.info(f"Traced Memory: size={traced_memory_size}, peak={traced_memory_peak}")
    num_unreacheable_objects = gc.collect()
    logger.info(f"Number of unreachable objects = {num_unreacheable_objects}")
    logger.info(f"GC count after collect {gc.get_count()}")

    # Collect unique traces of all live objects in the garbage - these have potential leaks.
    unique_traces_to_objects: Dict[
        Union[tracemalloc.Traceback, int], List[object]
    ] = defaultdict(list)
    for obj in gc.garbage:
        obj_trace = tracemalloc.get_object_traceback(obj)
        if obj_trace is not None:
            if _trace_has_file(obj_trace, "*datahub/*.py"):
                # Leaking object
                unique_traces_to_objects[obj_trace].append(obj)
            else:
                unique_traces_to_objects[id(obj)].append(obj)
    logger.info("Potentially leaking objects start")
    for key, obj_list in sorted(
        unique_traces_to_objects.items(),
        key=lambda item: sum([sys.getsizeof(o) for o in item[1]]),
        reverse=True,
    ):
        if isinstance(key, tracemalloc.Traceback):
            obj_traceback: tracemalloc.Traceback = cast(tracemalloc.Traceback, key)
            logger.info(
                f"#Objects:{len(obj_list)}; Total memory:{sum([sys.getsizeof(obj) for obj in obj_list])};"
                + " Allocation Trace:\n\t"
                + "\n\t".join(obj_traceback.format(limit=25))
            )
        else:
            logger.info(
                f"#Objects:{len(obj_list)}; Total memory:{sum([sys.getsizeof(obj) for obj in obj_list])};"
                + " No Allocation Trace available!"
            )
        # Print details about the live referrers of each object in the obj_list (same trace).
        for obj in obj_list:
            referrers = [r for r in gc.get_referrers(obj) if r in gc.garbage]
            logger.info(
                f"Referrers[{len(referrers)}] for object(addr={hex(id(obj))}):'{obj}':"
            )
            for ref_index, referrer in enumerate(referrers):
                ref_trace = tracemalloc.get_object_traceback(referrer)
                logger.info(
                    f"Referrer[{ref_index}] referrer_obj(addr={hex(id(referrer))}):{referrer}, RefTrace:\n\t\t"
                    + "\n\t\t".join(ref_trace.format(limit=5) if ref_trace else [])
                    + "\n"
                )
    logger.info("Potentially leaking objects end")

    tracemalloc.stop()


# TODO: Transition to ParamSpec with the first arg being click.Context (using typing_extensions.Concatenate)
#  once fully supported by mypy.
def with_leak_detection(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        detect_leaks: bool = args[0].obj.get("detect_memory_leaks", False)
        if detect_leaks:
            logger.info(
                f"Initializing memory leak detection on command: {func.__module__}.{func.__name__}"
            )
            _init_leak_detection()

        try:
            res = func(*args, **kwargs)
            return res
        finally:
            if detect_leaks:
                _perform_leak_detection()
                logger.info(
                    f"Finished memory leak detection on command: {func.__module__}.{func.__name__}"
                )

    return wrapper
