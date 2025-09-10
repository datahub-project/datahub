from datahub_integrations.experimentation.ai_init import (
    AI_EXPERIMENTATION_INITIALIZED,
)

import fnmatch
import pathlib
import time
from typing import Annotated, List, Optional

import asyncer
import mlflow
import pandas as pd
import typer
from anyio import to_thread
from datahub.sdk.main_client import DataHubClient
from loguru import logger
from mlflow import metrics as mlflow_metrics

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    HumanMessage,
)
from datahub_integrations.chat.chat_session import (
    CHATBOT_MODEL,
    ChatSession,
    NextMessage,
)
from datahub_integrations.experimentation.chatbot.chatbot import (
    Prompt,
    prompts as all_prompts,
    prompts_file,
)
from datahub_integrations.experimentation.creds import create_uncached_datahub_graph
from datahub_integrations.experimentation.utils import execute_notebook_save_as_html
from datahub_integrations.gen_ai.description_v3 import ANYIO_THREAD_COUNT
from datahub_integrations.mcp.mcp_server import mcp

assert AI_EXPERIMENTATION_INITIALIZED

experiments_dir = pathlib.Path(__file__).parent


@mlflow.trace
async def run_prompt(case: Prompt, local_results_dir: pathlib.Path) -> dict:
    mlflow.update_current_trace(tags={"prompt_id": case.id})
    # span = mlflow.get_current_active_span()
    # assert span is not None
    # span.set_attribute("prompt_id", case.id)

    graph = create_uncached_datahub_graph(key=case.instance)
    client = DataHubClient(graph=graph)
    try:
        history = ChatHistory(messages=[HumanMessage(text=case.message)])
        session = ChatSession(tools=[mcp], client=client, history=history)
        output_file = local_results_dir / f"{case.id}.json"

        next_message: NextMessage = await asyncer.asyncify(
            session.generate_next_message
        )()

        return {
            "response": next_message.text,
            "follow_up_suggestions": next_message.suggestions,
            "next_message": next_message.model_dump_json(),
            "history": history.model_dump_json(),
            "error": None,
        }
    except Exception as e:
        return {
            "response": None,
            "follow_up_suggestions": None,
            "next_message": None,
            "history": None,
            "error": str(e),
        }
    finally:
        history.save_file(output_file)
        mlflow.log_artifact(str(output_file), artifact_path="history")


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


async def main(
    prompt_ids: Annotated[Optional[List[str]], typer.Option(None)] = None,
) -> None:
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

    to_thread.current_default_thread_limiter().total_tokens = ANYIO_THREAD_COUNT
    with mlflow.start_run() as run:
        assert run.info.run_name is not None
        experiment_results_dir: pathlib.Path = (
            experiments_dir / "runs" / run.info.run_name
        )
        experiment_results_dir.mkdir(parents=True)

        mlflow.log_artifact(str(prompts_file))
        mlflow.log_params(
            {
                "model": CHATBOT_MODEL,
                "anyio_thread_count": ANYIO_THREAD_COUNT,
            }
        )

        tasks: list[tuple[Prompt, asyncer.SoonValue[dict]]] = []
        async with asyncer.create_task_group() as tg:
            for prompt in filtered_prompts:
                tasks.append(
                    (
                        prompt,
                        tg.soonify(run_prompt)(
                            prompt, local_results_dir=experiment_results_dir
                        ),
                    )
                )

        logger.info("All tasks finished")

        results = []
        for prompt, task in tasks:
            results.append(
                {
                    "prompt_id": prompt.id,
                    **prompt.model_dump(exclude={"id"}),
                    **task.value,
                }
            )
        results_df = pd.DataFrame(results)

        logger.info("Evaluating results")
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
        logger.debug(eval_result)
        try:
            html_path = execute_notebook_save_as_html(
                experiments_dir / "analyze_run.ipynb",
                pathlib.Path(experiment_results_dir),
                {"RUN_NAME": run.info.run_name},
            )
            mlflow.log_artifact(html_path)
        except Exception as e:
            logger.error(f"Error executing notebook: {e}")


if __name__ == "__main__":
    typer.run(asyncer.syncify(main, raise_sync_error=False))
    logger.info("Sleeping for 5 seconds to ensure logging is flushed")
    time.sleep(5)
