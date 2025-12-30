"""Decorator utilities for adding observability to functions.

This module provides reusable decorators following DRY principles to instrument
functions with metrics and tracing without code duplication.
"""

import functools
import inspect
import time
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, cast

from loguru import logger
from opentelemetry import metrics, trace
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import Status, StatusCode, Tracer

P = ParamSpec("P")
T = TypeVar("T")


# Import here to avoid circular dependency
def _lazy_import_cost_tracker():
    """Lazy import to avoid circular dependency at module load time."""
    from datahub_integrations.observability.cost import get_cost_tracker

    return get_cost_tracker()


def _lazy_import_ai_module():
    """Lazy import AIModule enum."""
    from datahub_integrations.observability.metrics_constants import AIModule

    return AIModule


def _get_meter(name: str) -> Meter:
    """Get or create a meter for the given module name."""
    return metrics.get_meter(name)


def _get_tracer(name: str) -> Tracer:
    """Get or create a tracer for the given module name."""
    return trace.get_tracer(name)


def _extract_labels_from_result(
    result: Any,
    label_extractors: dict[str, Callable[[Any], str]],
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Extract labels from function result using provided extractors.

    Args:
        result: Function return value.
        label_extractors: Map of label names to extractor functions.
        args: Positional arguments passed to the decorated function.
        kwargs: Keyword arguments passed to the decorated function.

    Returns:
        Dictionary of extracted labels.
    """
    labels: dict[str, str] = {}
    args = args or ()
    kwargs = kwargs or {}

    for label_name, extractor in label_extractors.items():
        try:
            # Try to call extractor with result, args, and kwargs
            # to support extractors that need function arguments
            labels[label_name] = extractor(result, *args, **kwargs)
        except Exception as e:
            logger.warning(f"Failed to extract label '{label_name}': {e}")
            labels[label_name] = "unknown"
    return labels


def otel_duration(
    metric_name: str,
    description: str = "",
    unit: str = "s",
    labels: dict[str, str] | None = None,
    label_extractors: dict[str, Callable[[Any], str]] | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to measure function execution duration using OpenTelemetry.

    Records a histogram metric with the function's duration.
    Supports both sync and async functions.

    Args:
        metric_name: Name of the histogram metric.
        description: Human-readable description of the metric.
        unit: Unit of measurement (default: seconds).
        labels: Static labels to attach to all observations.
        label_extractors: Functions to extract labels from return value.

    Returns:
        Decorator function.

    Example:
        ```python
        @otel_duration(
            "actions_execution_duration",
            description="Action execution time",
            labels={"action_type": "webhook"}
        )
        def run_action(spec: ActionSpec) -> ActionResult:
            ...
        ```
    """
    labels = labels or {}
    label_extractors = label_extractors or {}

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        meter = _get_meter(func.__module__)
        histogram: Histogram = meter.create_histogram(
            name=metric_name,
            description=description or f"Duration of {func.__name__}",
            unit=unit,
        )

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                start_time = time.perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    duration = time.perf_counter() - start_time

                    # Extract dynamic labels from result and function arguments
                    dynamic_labels = _extract_labels_from_result(
                        result, label_extractors, args, kwargs
                    )
                    all_labels = {**labels, **dynamic_labels}

                    histogram.record(duration, attributes=all_labels)
                    return result
                except Exception:
                    duration = time.perf_counter() - start_time
                    histogram.record(duration, attributes={**labels, "status": "error"})
                    raise

            return cast(Callable[P, T], async_wrapper)
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                start_time = time.perf_counter()
                try:
                    result = func(*args, **kwargs)
                    duration = time.perf_counter() - start_time

                    # Extract dynamic labels from result and function arguments
                    dynamic_labels = _extract_labels_from_result(
                        result, label_extractors, args, kwargs
                    )
                    all_labels = {**labels, **dynamic_labels}

                    histogram.record(duration, attributes=all_labels)
                    return result
                except Exception:
                    duration = time.perf_counter() - start_time
                    histogram.record(duration, attributes={**labels, "status": "error"})
                    raise

            return cast(Callable[P, T], sync_wrapper)

    return decorator


def otel_counter(
    metric_name: str,
    description: str = "",
    labels: dict[str, str] | None = None,
    label_extractors: dict[str, Callable[[Any], str]] | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to count function invocations using OpenTelemetry.

    Records a counter metric incremented on each function call.
    Supports both sync and async functions.

    Args:
        metric_name: Name of the counter metric.
        description: Human-readable description of the metric.
        labels: Static labels to attach to all observations.
        label_extractors: Functions to extract labels from return value.

    Returns:
        Decorator function.

    Example:
        ```python
        @otel_counter(
            "slack_command_total",
            description="Total Slack commands",
            labels={"command": "search"}
        )
        def handle_search(query: str) -> SearchResult:
            ...
        ```
    """
    labels = labels or {}
    label_extractors = label_extractors or {}

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        meter = _get_meter(func.__module__)
        counter: Counter = meter.create_counter(
            name=metric_name,
            description=description or f"Count of {func.__name__} calls",
        )

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                try:
                    result = await func(*args, **kwargs)

                    # Extract dynamic labels from result and function arguments
                    dynamic_labels = _extract_labels_from_result(
                        result, label_extractors, args, kwargs
                    )
                    all_labels = {**labels, **dynamic_labels, "status": "success"}

                    counter.add(1, attributes=all_labels)
                    return result
                except Exception:
                    counter.add(1, attributes={**labels, "status": "error"})
                    raise

            return cast(Callable[P, T], async_wrapper)
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                try:
                    result = func(*args, **kwargs)

                    # Extract dynamic labels from result and function arguments
                    dynamic_labels = _extract_labels_from_result(
                        result, label_extractors, args, kwargs
                    )
                    all_labels = {**labels, **dynamic_labels, "status": "success"}

                    counter.add(1, attributes=all_labels)
                    return result
                except Exception:
                    counter.add(1, attributes={**labels, "status": "error"})
                    raise

            return cast(Callable[P, T], sync_wrapper)

    return decorator


def otel_span(
    span_name: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to add OpenTelemetry distributed tracing span around function.

    Creates a span that tracks the function execution in distributed traces.
    Supports both sync and async functions.

    Args:
        span_name: Name of the span (defaults to function name).
        attributes: Additional span attributes.

    Returns:
        Decorator function.

    Example:
        ```python
        @otel_span(
            span_name="execute_action",
            attributes={"component": "actions_manager"}
        )
        def run_action(spec: ActionSpec) -> ActionResult:
            ...
        ```
    """
    attributes = attributes or {}

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        tracer = _get_tracer(func.__module__)
        name = span_name or func.__name__

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                with tracer.start_as_current_span(name, attributes=attributes) as span:
                    try:
                        result = await func(*args, **kwargs)
                        span.set_status(Status(StatusCode.OK))
                        return result
                    except Exception as e:
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        span.record_exception(e)
                        raise

            return cast(Callable[P, T], async_wrapper)
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                with tracer.start_as_current_span(name, attributes=attributes) as span:
                    try:
                        result = func(*args, **kwargs)
                        span.set_status(Status(StatusCode.OK))
                        return result
                    except Exception as e:
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        span.record_exception(e)
                        raise

            return cast(Callable[P, T], sync_wrapper)

    return decorator


def otel_instrument(
    metric_prefix: str,
    description: str = "",
    labels: dict[str, str] | None = None,
    trace: bool = True,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to add comprehensive OpenTelemetry instrumentation (duration + count + tracing).

    This is a convenience decorator that combines duration measurement,
    call counting, and distributed tracing.

    Args:
        metric_prefix: Prefix for metric names (e.g., "actions_execution").
        description: Human-readable description.
        labels: Static labels for metrics.
        trace: Whether to add distributed tracing span.

    Returns:
        Decorator function.

    Example:
        ```python
        @otel_instrument(
            metric_prefix="slack_command",
            labels={"command": "search"}
        )
        async def handle_search(query: str) -> SearchResult:
            ...
        ```

        This creates:
        - slack_command_duration (histogram)
        - slack_command_total (counter)
        - Distributed trace span
    """
    labels = labels or {}

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        # Stack decorators: trace -> duration -> count
        decorated = func

        # Add count decorator (innermost)
        decorated = otel_counter(
            metric_name=f"{metric_prefix}_total",
            description=f"{description} - total calls" if description else "",
            labels=labels,
        )(decorated)

        # Add duration decorator
        decorated = otel_duration(
            metric_name=f"{metric_prefix}_duration",
            description=f"{description} - duration" if description else "",
            labels=labels,
        )(decorated)

        # Add trace span (outermost)
        if trace:
            decorated = otel_span(
                span_name=f"{metric_prefix}",
                attributes=labels,
            )(decorated)

        return decorated

    return decorator


def otel_llm_call(
    ai_module: str = "chat",
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator to instrument LLM API calls with OpenTelemetry latency tracking.

    This decorator automatically records LLM call latency metrics by extracting
    provider and model information from the LLMWrapper instance (self).

    The decorator expects to be used on methods of classes that have:
    - self.provider_name (str): Provider name (e.g., "anthropic", "openai", "bedrock")
    - self.model_name (str): Model identifier

    Args:
        ai_module: AI module name (default: "chat"). Use AIModule enum values.

    Returns:
        Decorator function.

    Example:
        ```python
        class BedrockLLMWrapper(LLMWrapper):
            provider_name = "bedrock"

            @otel_llm_call(ai_module="chat")
            def converse(self, system, messages, ...):
                # LLM call here
                ...
        ```

    Note: This uses lazy imports to avoid circular dependencies.
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # Extract self (first argument should be the instance)
            if not args:
                # No self, just call the function
                return func(*args, **kwargs)

            self_obj = args[0]

            # Get provider and model from instance
            provider = getattr(self_obj, "provider_name", "unknown")
            model = getattr(self_obj, "model_name", "unknown")

            # Get tracker (lazy import to avoid circular dependency)
            tracker = _lazy_import_cost_tracker()
            AIModule = _lazy_import_ai_module()

            # Resolve ai_module string to enum
            try:
                ai_module_enum = AIModule(ai_module)
            except (ValueError, KeyError):
                logger.warning(f"Unknown AI module: {ai_module}, using CHAT")
                ai_module_enum = AIModule.CHAT

            # Time the call
            start_time = time.time()
            success = False

            try:
                result = func(*args, **kwargs)
                success = True
                return result
            except Exception:
                success = False
                raise
            finally:
                duration = time.time() - start_time
                tracker.record_llm_call_latency(
                    duration_seconds=duration,
                    provider=provider,
                    model=model,
                    ai_module=ai_module_enum,
                    success=success,
                )

        return cast(Callable[P, T], wrapper)

    return decorator
