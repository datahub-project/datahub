# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
from time import time
from typing import Dict

import click

from datahub_actions.action.action_stats import ActionStats
from datahub_actions.pipeline.pipeline_util import get_transformer_name
from datahub_actions.transform.transformer import Transformer
from datahub_actions.transform.transformer_stats import TransformerStats


# Class that stores running statistics for a single Actions Pipeline.
class PipelineStats:
    # Timestamp in milliseconds when the pipeline was launched.
    started_at: int

    # Number of events that failed processing even after retry.
    failed_event_count: int = 0

    # Number of events that failed when "ack" was invoked.
    failed_ack_count: int = 0

    # Top-level number of succeeded processing executions.
    success_count: int = 0

    # Transformer Stats
    transformer_stats: Dict[str, TransformerStats] = {}

    # Action Stats
    action_stats: ActionStats = ActionStats()

    def mark_start(self) -> None:
        self.started_at = int(time() * 1000)

    def increment_failed_event_count(self) -> None:
        self.failed_event_count = self.failed_event_count + 1

    def increment_failed_ack_count(self) -> None:
        self.failed_ack_count = self.failed_ack_count + 1

    def increment_success_count(self) -> None:
        self.success_count = self.success_count + 1

    def increment_transformer_exception_count(self, transformer: Transformer) -> None:
        transformer_name = get_transformer_name(transformer)
        if transformer_name not in self.transformer_stats:
            self.transformer_stats[transformer_name] = TransformerStats()
        self.transformer_stats[transformer_name].increment_exception_count()

    def increment_transformer_processed_count(self, transformer: Transformer) -> None:
        transformer_name = get_transformer_name(transformer)
        if transformer_name not in self.transformer_stats:
            self.transformer_stats[transformer_name] = TransformerStats()
        self.transformer_stats[transformer_name].increment_processed_count()

    def increment_transformer_filtered_count(self, transformer: Transformer) -> None:
        transformer_name = get_transformer_name(transformer)
        if transformer_name not in self.transformer_stats:
            self.transformer_stats[transformer_name] = TransformerStats()
        self.transformer_stats[transformer_name].increment_filtered_count()

    def increment_action_exception_count(self) -> None:
        self.action_stats.increment_exception_count()

    def increment_action_success_count(self) -> None:
        self.action_stats.increment_success_count()

    def get_started_at(self) -> int:
        return self.started_at

    def get_failed_event_count(self) -> int:
        return self.failed_event_count

    def get_failed_ack_count(self) -> int:
        return self.failed_ack_count

    def get_success_count(self) -> int:
        return self.success_count

    def get_transformer_stats(self, transformer: Transformer) -> TransformerStats:
        transformer_name = get_transformer_name(transformer)
        if transformer_name not in self.transformer_stats:
            self.transformer_stats[transformer_name] = TransformerStats()
        return self.transformer_stats[transformer_name]

    def get_action_stats(self) -> ActionStats:
        return self.action_stats

    def as_string(self) -> str:
        return json.dumps(self.__dict__, indent=4, sort_keys=True)

    def pretty_print_summary(self, name: str) -> None:
        curr_time = int(time() * 1000)
        click.echo()
        click.secho(f"Pipeline Report for {name}", bold=True, fg="blue")
        click.echo()
        click.echo(
            f"Started at: {datetime.datetime.fromtimestamp(self.started_at / 1000.0)} (Local Time)"
        )
        click.echo(f"Duration: {(curr_time - self.started_at) / 1000.0}s")
        click.echo()
        click.secho("Pipeline statistics", bold=True)
        click.echo()
        click.echo(self.as_string())
        click.echo()
        if len(self.transformer_stats.keys()) > 0:
            click.secho("Transformer statistics", bold=True)
            for key in self.transformer_stats:
                click.echo()
                click.echo(f"{key}: {self.transformer_stats[key].as_string()}")
            click.echo()
        click.secho("Action statistics", bold=True)
        click.echo()
        click.echo(self.action_stats.as_string())
        click.echo()
