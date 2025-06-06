import pathlib
import time
from typing import Annotated, List, Optional

import asyncer
import mlflow
import pandas as pd
import typer
from datahub.sdk.main_client import DataHubClient
from loguru import logger
from mlflow import metrics as mlflow_metrics

from datahub_integrations.chat.chat_history import (
    ChatHistory,
    HumanMessage,
)
from datahub_integrations.chat.chat_session import ChatSession, NextMessage
from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED
from datahub_integrations.experimentation.chatbot import Prompt, prompts, prompts_file
from datahub_integrations.experimentation.creds import create_uncached_datahub_graph
from datahub_integrations.mcp.mcp_server import mcp

assert AI_EXPERIMENTATION_INITIALIZED

experiments_dir = pathlib.Path(__file__).parent


@mlflow.trace
async def run_prompt(case: Prompt, local_results_dir: pathlib.Path) -> dict:
    mlflow.update_current_trace(tags={"prompt_id": case.id})
    # span = mlflow.get_current_active_span()
    # assert span is not None
    # span.set_attribute("prompt_id", case.id)

    try:
        graph = create_uncached_datahub_graph(key=case.instance)
        client = DataHubClient(graph=graph)
        history = ChatHistory(messages=[HumanMessage(text=case.message)])
        session = ChatSession(tools=mcp.get_all_tools(), client=client, history=history)

        next_message: NextMessage = await asyncer.asyncify(
            session.generate_next_message
        )()

        output_file = local_results_dir / f"{case.id}.json"
        history.save_file(output_file)
        mlflow.log_artifact(str(output_file), artifact_path="history")

        return {
            "response": next_message.text,
            "follow_up_suggestions": next_message.suggestions,
            "next_message": next_message.json(),
            "history": history.json(),
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


def _has_response_metric_fn(predictions, targets, metrics):
    scores = [float(pred is not None and pred != "") for pred in predictions]
    return mlflow_metrics.MetricValue(
        scores=scores,
        aggregate_results={
            "pass_percentage": len(list(filter(lambda x: x, scores))) / len(scores)
        },
    )


has_response_metric = mlflow_metrics.make_metric(
    eval_fn=_has_response_metric_fn,
    name="has_response",
    greater_is_better=True,
)


async def main(prompt_ids: Annotated[Optional[List[str]], typer.Option(None)] = None):
    filtered_prompts = prompts
    if prompt_ids:
        filtered_prompts = [p for p in prompts if p.id in prompt_ids]
        missing_prompt_ids = set(prompt_ids) - set([p.id for p in filtered_prompts])
        assert len(missing_prompt_ids) == 0, f"Unknown prompt ids: {missing_prompt_ids}"
        logger.info(f"Running {len(filtered_prompts)} prompts")

    with mlflow.start_run() as run:
        assert run.info.run_name is not None
        experiment_results_dir: pathlib.Path = (
            experiments_dir / "runs" / run.info.run_name
        )
        experiment_results_dir.mkdir(parents=True)

        mlflow.log_artifact(str(prompts_file))

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
                    **prompt.dict(exclude={"id"}),
                    **task.value,
                }
            )
        results_df = pd.DataFrame(results)

        logger.info("Evaluating results")
        eval_result = mlflow.evaluate(
            data=results_df,
            predictions="response",
            evaluators="default",
            targets="response_guidelines",
            extra_metrics=[
                has_response_metric,
                # mlflow.metrics.token_count(),
            ],
        )
        logger.debug(eval_result)


if __name__ == "__main__":
    typer.run(asyncer.syncify(main, raise_sync_error=False))
    logger.info("Sleeping for 5 seconds to ensure logging is flushed")
    time.sleep(5)
