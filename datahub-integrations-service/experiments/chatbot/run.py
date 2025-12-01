from datahub.utilities.perf_timer import PerfTimer
from datahub_integrations.experimentation.ai_init import (
    AI_EXPERIMENTATION_INITIALIZED,
)

import fnmatch
import os
import pathlib
import platform
import socket
import time
from typing import Annotated, Dict, List, Optional

import asyncer
import mlflow
import more_itertools
import pandas as pd
import typer
import yaml
from anyio import to_thread
from datahub.sdk.main_client import DataHubClient
from loguru import logger
from mlflow import metrics as mlflow_metrics

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    HumanMessage,
)
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.chat.agent import AgentRunner
from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
from datahub_integrations.chat.types import NextMessage
from datahub_integrations.experimentation.chatbot.chatbot import (
    Prompt,
    prompts as all_prompts,
    prompts_file,
)
from datahub_integrations.experimentation.creds import create_uncached_datahub_graph
from datahub_integrations.experimentation.utils import execute_notebook_save_as_html
from datahub_integrations.gen_ai.description_v3 import ANYIO_THREAD_COUNT

assert AI_EXPERIMENTATION_INITIALIZED

experiments_dir = pathlib.Path(__file__).parent

# Control parallelism by batching prompt execution rather than limiting threads
BATCH_SIZE = 25  # Number of prompts to run concurrently per batch


def load_instruction_overrides() -> Optional[Dict[str, str]]:
    """Load instruction overrides if the file exists."""
    override_file = experiments_dir / "instructions-override.yaml"
    if override_file.exists():
        logger.info(f"Loading instruction overrides from {override_file}")
        with open(override_file) as f:
            content = yaml.safe_load(f)
            # If content is None or empty, return None
            if not content:
                return None
            return content
    return None


@mlflow.trace
async def run_prompt(
    case: Prompt,
    local_results_dir: pathlib.Path,
    instruction_overrides: Optional[Dict[str, str]] = None,
) -> dict:
    # Add experiment context to all logs within this function
    with logger.contextualize(prompt_id=case.id, instance=case.instance):
        logger.info(f"Starting experiment for prompt: {case.id}")
        logger.debug(f"Prompt message: {case.message}")
        logger.debug(f"Using instance: {case.instance}")

        mlflow.update_current_trace(tags={"prompt_id": case.id})
        # span = mlflow.get_current_active_span()
        # assert span is not None
        # span.set_attribute("prompt_id", case.id)

        logger.debug("Creating DataHub graph connection")
        graph = create_uncached_datahub_graph(key=case.instance)
        client = DataHubClient(graph=graph)

        try:
            # Get override for this instance if available
            extra_instructions_override = None
            if instruction_overrides and case.instance in instruction_overrides:
                logger.info(f"Using instruction override for instance: {case.instance}")
                extra_instructions_override = instruction_overrides[case.instance]

            logger.debug("Setting up agent")
            history = ChatHistory(messages=[HumanMessage(text=case.message)])
            agent = create_data_catalog_explorer_agent(
                client=client,
                history=history,
                extra_instructions_override=extra_instructions_override,
            )
            output_file = local_results_dir / f"{case.id}.json"

            logger.debug("Generating chatbot response")
            with PerfTimer() as timer:
                next_message: NextMessage = await asyncer.asyncify(
                    agent.generate_formatted_message
                )()
                response_time = timer.elapsed_seconds()

            logger.info(f"Successfully generated response for prompt: {case.id}")
            logger.debug(
                f"Response length: {len(next_message.text) if next_message.text else 0} chars"
            )
            logger.debug(f"Number of suggestions: {len(next_message.suggestions)}")

            return {
                "response": next_message.text,
                "follow_up_suggestions": next_message.suggestions,
                "next_message": next_message.model_dump_json(),
                "history": history.model_dump_json(),
                "error": None,
                "response_time": response_time,
            }
        except Exception as e:
            logger.error(f"Error in prompt {case.id}: {str(e)}")
            logger.debug(f"Exception details for {case.id}: {type(e).__name__}: {e}")
            return {
                "response": None,
                "follow_up_suggestions": None,
                "next_message": None,
                "history": None,
                "error": str(e),
                "response_time": None,
            }
        finally:
            logger.debug(f"Saving history file for {case.id}")
            history.save_file(output_file)
            mlflow.log_artifact(str(output_file), artifact_path="history")
            logger.debug(f"Completed experiment for prompt: {case.id}")


def _has_response_metric_fn(predictions: pd.Series, targets: pd.Series, metrics: dict):
    scores = [float(resp is not None and resp != "") for resp in predictions]
    return mlflow_metrics.MetricValue(
        scores=scores,
        aggregate_results={
            "pass_percentage": 100
            * len(list(filter(lambda x: x, scores)))
            / len(scores)
            if len(scores) > 0
            else 0,
            "count": len(scores),
        },
    )


async def process_batch(
    batch: List[Prompt],
    batch_idx: int,
    total_batches: int,
    experiment_results_dir: pathlib.Path,
    instruction_overrides: Optional[Dict[str, str]] = None,
) -> List[tuple[Prompt, dict]]:
    """Process a single batch of prompts concurrently."""
    logger.info(
        f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} prompts)"
    )

    tasks: List[tuple[Prompt, asyncer.SoonValue[dict]]] = []

    async with asyncer.create_task_group() as tg:
        for prompt in batch:
            logger.debug(f"Queuing experiment task for prompt: {prompt.id}")
            tasks.append(
                (
                    prompt,
                    tg.soonify(run_prompt)(
                        prompt,
                        local_results_dir=experiment_results_dir,
                        instruction_overrides=instruction_overrides,
                    ),
                )
            )

    # Collect results from this batch
    batch_results = []
    for prompt, task in tasks:
        batch_results.append((prompt, task.value))

    logger.info(f"Completed batch {batch_idx + 1}/{total_batches}")
    return batch_results


async def process_all_batches(
    filtered_prompts: List[Prompt],
    experiment_results_dir: pathlib.Path,
    instruction_overrides: Optional[Dict[str, str]] = None,
) -> List[tuple[Prompt, dict]]:
    """Process all prompts in batches and return results."""
    logger.info(
        f"Starting batched execution of {len(filtered_prompts)} experiments (batch size: {BATCH_SIZE})"
    )

    instances = set(prompt.instance for prompt in filtered_prompts)
    logger.info(f"Checking connection to instances: {instances}")
    for instance in instances:
        try:
            create_uncached_datahub_graph(key=instance)
        except Exception as e:
            logger.error(f"Error creating graph for instance: {instance}")
            logger.error(f"Error details: {e}")
            raise

    logger.info("Good to go!")

    # Split prompts into batches to control parallelism
    prompt_batches = list(more_itertools.chunked(filtered_prompts, BATCH_SIZE))
    logger.info(
        f"Split {len(filtered_prompts)} prompts into {len(prompt_batches)} batches"
    )

    all_results = []

    for batch_idx, batch in enumerate(prompt_batches):
        batch_results = await process_batch(
            batch,
            batch_idx,
            len(prompt_batches),
            experiment_results_dir,
            instruction_overrides,
        )
        all_results.extend(batch_results)

    logger.info(
        f"All {len(all_results)} tasks have been completed across {len(prompt_batches)} batches"
    )
    return all_results

def log_generation_time_metrics(evals_df: pd.DataFrame) -> None:
    """Log generation time metrics to MLflow."""
    response_times = evals_df["response_time"].dropna()
    if len(response_times) > 0:
        mlflow.log_metric("response_time_max", response_times.max())
        mlflow.log_metric("response_time_avg", response_times.mean())


async def main(
    prompt_ids: Optional[List[str]] = None,
    instances: Optional[List[str]] = None,
) -> None:
    # Configure file logging with DEBUG level
    log_file = (
        experiments_dir / "logs" / f"chatbot_eval_{time.strftime('%Y%m%d_%H%M%S')}.log"
    )
    log_file.parent.mkdir(exist_ok=True)

    # Remove the default loguru handler (which goes to stderr)
    # and replace with our own console handler at INFO level
    logger.remove()

    # Add console handler with INFO level (clean output)
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level="INFO",
        format="{time:HH:mm:ss} | {level: <8} | {message}",
        colorize=True,
    )

    # Filter function to add default context values when missing for file logs
    def add_default_context(record):
        if "prompt_id" not in record["extra"]:
            record["extra"]["prompt_id"] = "MAIN"
        if "instance" not in record["extra"]:
            record["extra"]["instance"] = "N/A"
        return True

    # Add file handler with DEBUG level and detailed format including experiment context
    logger.add(
        log_file,
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {extra[prompt_id]:<15} | {extra[instance]:<10} | {name}:{function}:{line} - {message}",
        filter=add_default_context,
        rotation="10 MB",
        retention="7 days",
    )

    logger.info(
        f"Starting chatbot evaluation - detailed logs will be written to: {log_file}"
    )
    logger.debug("Debug logging enabled for detailed troubleshooting")

    # Validate that only one of prompt_ids or instances is provided
    if prompt_ids and instances:
        logger.error("Cannot specify both --prompt-ids and --instances. Please provide only one.")
        raise typer.BadParameter("Cannot specify both --prompt-ids and --instances. Please provide only one.")

    filtered_prompts = all_prompts
    if prompt_ids:
        filtered_prompts = [
            p
            for p in all_prompts
            if any(fnmatch.fnmatch(p.id, pattern) for pattern in prompt_ids)
        ]
        if len(filtered_prompts) == 0:
            logger.error(f"No prompts found for {prompt_ids}")
            return
        logger.info(f"Running {len(filtered_prompts)} prompts")
        logger.debug(f"Filtered prompts: {[p.id for p in filtered_prompts]}")
    elif instances:
        filtered_prompts = [
            p
            for p in all_prompts
            if any(fnmatch.fnmatch(p.instance, pattern) for pattern in instances)
        ]
        if len(filtered_prompts) == 0:
            logger.error(f"No prompts found for instances {instances}")
            return
        logger.info(f"Running {len(filtered_prompts)} prompts from instances: {instances}")
        logger.debug(f"Filtered prompts: {[p.id for p in filtered_prompts]}")

    # Load instruction overrides once at the start
    instruction_overrides = load_instruction_overrides()
    if instruction_overrides:
        logger.info(
            f"Loaded instruction overrides for instances: {list(instruction_overrides.keys())}"
        )

    to_thread.current_default_thread_limiter().total_tokens = ANYIO_THREAD_COUNT
    with mlflow.start_run() as run:
        assert run.info.run_name is not None
        experiment_results_dir: pathlib.Path = (
            experiments_dir / "runs" / run.info.run_name
        )
        experiment_results_dir.mkdir(parents=True)

        # Tag run with machine/environment metadata for debugging and tracking
        mlflow.set_tags({
            "machine.hostname": socket.gethostname(),
            "machine.user": os.getenv("USER") or os.getenv("USERNAME") or "unknown",
            "machine.os": f"{platform.system()} {platform.release()}",
            "machine.python_version": platform.python_version(),
        })

        mlflow.log_artifact(str(prompts_file))
        mlflow.log_params(
            {
                "model": model_config.chat_assistant_ai.model,
                "anyio_thread_count": ANYIO_THREAD_COUNT,
                "batch_size": BATCH_SIZE,
                "has_instruction_overrides": instruction_overrides is not None,
            }
        )

        # Process all prompts in batches
        task_results = await process_all_batches(
            filtered_prompts, experiment_results_dir, instruction_overrides
        )

        logger.debug(f"Processing results for {len(task_results)} completed tasks")
        results = []
        successful_tasks = 0
        failed_tasks = 0

        for prompt, task_result in task_results:
            results.append(
                {
                    "prompt_id": prompt.id,
                    **prompt.model_dump(exclude={"id"}),
                    **task_result,
                }
            )

            # Track success/failure
            if task_result.get("error") is None:
                successful_tasks += 1
                logger.debug(f"Task {prompt.id}: SUCCESS")
            else:
                failed_tasks += 1
                logger.debug(f"Task {prompt.id}: FAILED - {task_result.get('error')}")

        logger.info(
            f"Task completion summary: {successful_tasks} successful, {failed_tasks} failed"
        )
        results_df = pd.DataFrame(results)

        logger.info("Evaluating results")
        logger.debug(f"Results dataframe shape: {results_df.shape}")
        eval_result = mlflow.evaluate(
            data=results_df,
            predictions="response",
            evaluators="default",
            targets="prompt_id",  # this is not used for evaluation
            extra_metrics=[
                mlflow_metrics.make_metric(
                    eval_fn=_has_response_metric_fn,
                    name="has_response",
                    greater_is_better=True,
                ),
                # mlflow.metrics.token_count(),
            ],
        )
        log_generation_time_metrics(results_df)
        logger.info("Evaluation completed successfully")
        logger.debug(f"Evaluation results: {eval_result}")

        logger.debug("Generating analysis notebook")
        try:
            html_path = execute_notebook_save_as_html(
                experiments_dir / "analyze_run.ipynb",
                pathlib.Path(experiment_results_dir),
                {"RUN_NAME": run.info.run_name},
            )
            mlflow.log_artifact(html_path)
            logger.info(f"Analysis notebook generated: {html_path}")
        except Exception as e:
            logger.error(f"Error executing notebook: {e}")

        logger.info(f"Experiment run completed: {run.info.run_name}")
        logger.info(f"Results saved to: {experiment_results_dir}")
        logger.info(f"Detailed logs saved to: {log_file}")


if __name__ == "__main__":
    typer.run(asyncer.syncify(main, raise_sync_error=False))
    logger.info("Sleeping for 5 seconds to ensure logging is flushed")
    time.sleep(5)
