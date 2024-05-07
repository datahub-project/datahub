import contextlib
import itertools
import json
import pathlib
import random
import string
from io import StringIO
from typing import Iterator, List, Optional, TypeVar

import anyio
import yaml
from datahub.utilities.ordered_set import OrderedSet
from datahub.utilities.yaml_sync_utils import YamlFileUpdater
from loguru import logger
from ruamel.yaml.comments import CommentedMap

from datahub_integrations.dispatch.runner import (
    VENV_NO_DATAHUB,
    LogHolder,
    SubprocessRunner,
    VenvConfig,
    VenvReference,
    setup_venv,
)

_FAKE_DBT_PROFILES = yaml.safe_load(
    StringIO((pathlib.Path(__file__).parent / "fake_dbt_profiles.yml").read_text())
)
_DEFAULT_DBT_PLATFORM = "postgres"

_D = TypeVar("_D", bound=dict)


def get_where_field_matches(doc: list[_D], field: str, value: str) -> Optional[_D]:
    return next(
        (item for item in doc if item[field] == value),
        None,
    )


def get_where_name_matches(doc: list[_D], name: str) -> Optional[_D]:
    return get_where_field_matches(doc, "name", name)


class DbtProject:
    def __init__(
        self,
        dbt_dir: pathlib.Path,
        base_temp_dir: pathlib.Path,
        target_platform: Optional[str] = None,
    ):
        self.dbt_dir = dbt_dir
        self.target_platform = target_platform
        self.base_temp_dir = base_temp_dir
        self.temp_profiles_dir = base_temp_dir / "profiles"
        self.temp_profiles_dir.mkdir(exist_ok=True)

        # On init, we setup the venv and install dbt / other things as required.
        log_holder = LogHolder(echo_to_stdout_prefix="", max_log_lines=None)
        self._runner = SubprocessRunner(log_holder)
        self._venv = self._make_dbt_runner()
        self._dbt_executable = self._venv.command("dbt")

        self.run_dbt("--version")
        self.run_dbt("deps")

    def read_dbt_project_yml(self) -> dict:
        with pathlib.Path(self.dbt_dir / "dbt_project.yml").open() as f:
            dbt_project = yaml.safe_load(f)

        return dbt_project

    def _read_dbt_profile_name(self) -> str:
        dbt_project = self.read_dbt_project_yml()
        return dbt_project["profile"]

    def _get_profile_dir(self, target_platform: str) -> pathlib.Path:
        profile_inner_config = _FAKE_DBT_PROFILES.get(
            f"DBT_PROFILE_NAME_{target_platform}"
        )
        if not profile_inner_config:
            raise ValueError(f"Unknown target platform: {target_platform}")
        profile_contents = {self._read_dbt_profile_name(): profile_inner_config}

        random_suffix = "".join(random.choices(string.ascii_lowercase, k=8))
        profile_dir = self.temp_profiles_dir / f"{target_platform}_{random_suffix}"
        profile_dir.mkdir(exist_ok=True)
        profile_file = profile_dir / "profiles.yml"
        with profile_file.open("w") as f:
            yaml.dump(profile_contents, f)
        logger.debug(f"Created fake dbt profiles file at {profile_file}")
        return profile_dir

    def _make_venv_config(self) -> VenvConfig:
        requirements_file = self.dbt_dir / "requirements.txt"
        if requirements_file.exists():
            logger.debug(f"Using dbt project requirements file: {requirements_file}")
            return VenvConfig(
                requirements_file=requirements_file,
                extra_env_vars={
                    "DBT_PROFILES_DIR": str(
                        self._get_profile_dir(
                            self.target_platform or _DEFAULT_DBT_PLATFORM
                        )
                    )
                },
            )

        else:
            # The target platform is mainly used for generating a "fake" profiles.yml
            # so that dbt doesn't throw an error. We don't actually need those credentials.
            return VenvConfig(
                version=VENV_NO_DATAHUB,
                extra_pip_requirements=[
                    "dbt",
                    f"dbt-{_DEFAULT_DBT_PLATFORM}",
                ],
                extra_env_vars={
                    "DBT_PROFILES_DIR": str(
                        self._get_profile_dir(_DEFAULT_DBT_PLATFORM)
                    )
                },
            )

    def _make_dbt_runner(self) -> VenvReference:
        venv_config = self._make_venv_config()

        if (self.dbt_dir / "venv/bin/dbt").exists():
            # If there's already a venv, we're probably running locally.
            # In that case, we can just use the existing venv.
            return VenvReference(
                venv_loc=self.dbt_dir / "venv",
                venv_config=venv_config,
            )
        else:
            return anyio.run(
                lambda: setup_venv(
                    venv_config=venv_config,
                    runner=self._runner,
                    tmp_dir=self.base_temp_dir,
                )
            )

    def run_dbt(self, *args: str, echo_stdout: bool = True) -> str:
        # TODO: This makes the assumption that the dbt command will not exceed the
        # line length limits of the log holder.

        self._runner.logs.clear()

        with contextlib.ExitStack() as stack:
            if not echo_stdout:
                stack.enter_context(self._runner.logs.changed_echo_prefix(None))

            anyio.run(
                lambda: self._runner.execute(
                    [
                        self._dbt_executable,
                        *args,
                    ],
                    env=self._venv.extra_envs(),
                    cwd=self.dbt_dir,
                )
            )

        return self._runner.logs.get_logs(skip_lines=1)


class DbtFileLocator:
    def __init__(self, dbt_project: DbtProject):
        self.dbt_project = dbt_project

    @property
    def dbt_dir(self) -> pathlib.Path:
        return self.dbt_project.dbt_dir

    def _get_dbt_metadata(self) -> List[dict]:
        res = self.dbt_project.run_dbt("ls", "--output", "json", echo_stdout=False)

        nodes = []
        for line in res.splitlines():
            if not line.startswith("{"):
                logger.debug(f"Skipping line: {line}")
                continue

            node = json.loads(line)
            nodes.append(node)

        return nodes

    def _list_dbt_yml_files_ordered(self) -> List[pathlib.Path]:
        dbt_project = self.dbt_project.read_dbt_project_yml()
        yml_paths = OrderedSet(
            itertools.chain(
                dbt_project["model-paths"],
                dbt_project["snapshot-paths"],
                dbt_project.get("seed-paths", []),
                dbt_project.get("analysis-paths", []),
                dbt_project.get("macro-paths", []),
            ),
        )

        yml_files = []
        for model_path in yml_paths:
            yml_files.extend((self.dbt_dir / model_path).glob("**/*.yml"))

        # Order these by specificity, so that we can find the most specific one.
        # e.g. if we have a model in models/foo/bar.sql, we want to find the
        # model in models/foo/bar.yml, not models/foo.yml.
        return list(sorted(yml_files, key=lambda p: len(p.parts), reverse=True))

    def _get_source_table_by_id(
        self, doc: CommentedMap, dbt_source_unique_id: str
    ) -> CommentedMap:
        _source, _proj, source_name, table_name = dbt_source_unique_id.split(".")
        # TODO What happens if proj is not the same as the dbt project name?

        sources = doc["sources"]
        source_set = get_where_name_matches(sources, source_name)
        if not source_set:
            raise ValueError(f"Unable to find source {source_name}")

        table = get_where_name_matches(source_set["tables"], table_name)
        if not table:
            raise ValueError(f"Unable to find table {table_name}")

        return table

    @classmethod
    def get_node_type(cls, dbt_unique_id: str) -> str:
        return dbt_unique_id.split(".")[0]

    @contextlib.contextmanager
    def get_dbt_yml_config_for_unique_id(
        self, dbt_unique_id: str
    ) -> Iterator[CommentedMap]:
        nodes = self._get_dbt_metadata()
        node = get_where_field_matches(nodes, "unique_id", dbt_unique_id)
        if not node:
            raise ValueError(f"Unable to find node {dbt_unique_id}")

        node_type = self.get_node_type(dbt_unique_id)
        plural_node_type = f"{node_type}s"

        if node_type == "source":
            yml_file = self.dbt_dir / node["original_file_path"]

            with YamlFileUpdater(yml_file) as doc:
                table = self._get_source_table_by_id(doc, dbt_unique_id)
                yield table

        elif node_type in {"model", "seed", "snapshot"}:
            # For models, the original_file_path points at the .sql file.
            # We need to find the highest-level .yml file that contains this model.

            # FIXME: model unique IDs can be multipart, so this doesn't work fully.
            # In particular, it breaks down when the model is in a subdirectory.
            model_name = dbt_unique_id.split(".")[-1]

            yml_file = None
            for candidate_yml_file in self._list_dbt_yml_files_ordered():
                with candidate_yml_file.open() as f:
                    doc = yaml.safe_load(f)

                if doc and plural_node_type in doc:
                    model = get_where_name_matches(doc[plural_node_type], model_name)
                    if model:
                        yml_file = candidate_yml_file
                        break

            # If no such yml file exists, we'll add one called "schema.yml" in the
            # same directory as the sql.
            # As per https://discourse.getdbt.com/t/advantages-of-one-monolithic-schema-yml-file-vs-multiple/5240
            # it seems like one yml per directory is the generally accepted standard.
            if not yml_file:
                model_path: pathlib.Path = self.dbt_dir / node["original_file_path"]
                model_dir = model_path.parent
                yml_file = model_dir / "schema.yml"

                # Create the file.
                yml_file.write_text(f"{plural_node_type}: []\n")

            with YamlFileUpdater(yml_file) as doc:
                doc.setdefault(plural_node_type, [])
                model = get_where_name_matches(doc[plural_node_type], model_name)

                if not model:
                    model = CommentedMap(name=model_name)
                    doc[plural_node_type].append(model)

                yield model
        else:
            raise ValueError(f"Unknown dbt node type: unique ID {dbt_unique_id}")


_DBT_TAG_NORMAL_TYPES = {"model", "snapshot"}
_DBT_TAG_TOP_LEVEL_TYPES = {"source", "exposure"}


def locate_tags(dbt_unique_id: str, doc: CommentedMap) -> List[str]:
    node_type = DbtFileLocator.get_node_type(dbt_unique_id)
    if node_type in _DBT_TAG_TOP_LEVEL_TYPES:
        tags = doc.setdefault("tags", [])
        if isinstance(doc["tags"], str):
            doc["tags"] = [doc["tags"]]
            tags = doc["tags"]
        return tags
    elif node_type in _DBT_TAG_NORMAL_TYPES:
        config = doc.setdefault("config", {})
        tags = config.setdefault("tags", [])
        if isinstance(config["tags"], str):
            config["tags"] = [config["tags"]]
            tags = config["tags"]
        return tags
    else:
        raise NotImplementedError(f"We don't support tags on {node_type} yet")


_DBT_META_CONFIG_TYPES = {"model", "seed", "snapshot", "source"}


def locate_meta(dbt_unique_id: str, doc: CommentedMap) -> dict:
    node_type = DbtFileLocator.get_node_type(dbt_unique_id)
    if node_type in _DBT_META_CONFIG_TYPES:
        config = doc.setdefault("config", {})
        meta = config.setdefault("meta", {})
        return meta
    else:
        raise NotImplementedError(f"We don't support meta on {node_type} yet")


def locate_datahub_meta(dbt_unique_id: str, doc: CommentedMap) -> dict:
    meta = locate_meta(dbt_unique_id, doc)
    datahub = meta.setdefault("datahub", {})
    return datahub
