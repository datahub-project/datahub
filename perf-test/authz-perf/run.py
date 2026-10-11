#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

HARNESS_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(HARNESS_ROOT))

from executor import RunConfig, run_all_personas
from lib.credentials import credentials_allow_prompt, resolve_credentials
from lib.datapack import ensure_datapack
from lib.expectations import adapt_benchmarks_for_view_auth
from lib.gms_config import fetch_gms_config
from lib.graphql import log
from lib.graphql_adapt import setup_graphql_queries
from lib.metadata import (
    capture_deployment_metadata,
    capture_fixture_metadata,
    capture_git_metadata,
    metadata_dict,
)
from lib.orchestration import ManifestRunEntry, git_dict, write_manifest
from lib.output_paths import build_slug, result_paths
from lib.paths import (
    default_fixture_dir,
    read_pack_index_version,
    resolve_datapack_dir,
)
from lib.persona_credentials import (
    ensure_persona_passwords,
    should_set_persona_passwords,
)
from lib.personas import load_benchmark_pack, load_personas_oracle
from lib.results import JsonlWriter, new_run_id, write_summary
from lib.system_info import (
    SystemInfoSnapshot,
    fetch_system_info,
    warn_authorization_settings_drift,
)
from lib.targets import (
    TargetSpec,
    parse_targets_from_args,
    resolve_frontend_url,
    validate_target_list,
)
from lib.variants import RunVariant, parse_variants, variant_matrix_size


@dataclass
class SharedRunArgs:
    fixture_dir: Path
    datapack_dir: Optional[Path]
    persona_list: list[str]
    warmup: int
    iterations: int
    cache_phases: list[str]
    parallel_personas: int
    full_correctness: bool
    es_jitter: bool
    force_reload: bool
    skip_load: bool
    allow_downgrade: bool
    username: Optional[str]
    password: Optional[str]
    set_persona_passwords: Optional[bool]
    repo_root: Path


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Authz persona performance benchmark")
    parser.add_argument(
        "--gms-url", default=None, help="GMS URL (default: from datahub env)"
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Coordinator username (default: ~/.datahubenv token, else prompt)",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Coordinator password (used with --username to mint a GMS token)",
    )
    parser.add_argument("--frontend-url", default=None, help="Frontend URL override")
    parser.add_argument(
        "--env-file",
        action="append",
        default=[],
        help="Alternate ~/.datahubenv-style YAML (repeatable)",
    )
    parser.add_argument(
        "--env-file-name",
        action="append",
        default=[],
        help="Override target name for paired --env-file (repeatable)",
    )
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Explicit target as key=value,... (repeatable)",
    )
    parser.add_argument(
        "--fixture-dir",
        type=Path,
        default=None,
        help="Perf harness fixture directory (personas.json, benchmarks.json)",
    )
    parser.add_argument(
        "--datapack-dir",
        type=Path,
        default=None,
        help="Local authz-perf-medium datapack directory (index.json, mcps/)",
    )
    parser.add_argument(
        "--personas",
        default=None,
        help="Comma-separated persona names (default: all 17)",
    )
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=25)
    parser.add_argument(
        "--cache-phases",
        default="warm",
        help="Comma-separated cache phases: warm,cold",
    )
    parser.add_argument("--parallel-personas", type=int, default=1)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Append-only JSONL output path (single-target mode)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for per-(target×variant) JSONL files and manifest",
    )
    parser.add_argument(
        "--docker-tag",
        action="append",
        default=[],
        help="Docker tag variant label (repeatable; records metadata per variant)",
    )
    parser.add_argument(
        "--run-label",
        action="append",
        default=[],
        help="Explicit build variant label (repeatable)",
    )
    parser.add_argument("--force-reload", action="store_true")
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--allow-downgrade", action="store_true")
    parser.add_argument("--full-correctness", action="store_true")
    parser.add_argument("--es-jitter", action="store_true")
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop orchestration on first target×variant failure",
    )
    parser.add_argument(
        "--set-persona-passwords",
        action="store_true",
        help="Admin-reset persona passwords before benchmarks (auto for --env-file/remotes)",
    )
    parser.add_argument(
        "--skip-set-persona-passwords",
        action="store_true",
        help="Do not admin-reset persona passwords even on remotes",
    )
    return parser.parse_args(argv)


def _validate_cli(args: argparse.Namespace) -> None:
    if args.output and args.output_dir:
        raise SystemExit("Use --output or --output-dir, not both.")
    if not args.output and not args.output_dir:
        raise SystemExit("One of --output or --output-dir is required.")


def _build_shared_args(args: argparse.Namespace) -> SharedRunArgs:
    fixture_dir = (args.fixture_dir or default_fixture_dir(HARNESS_ROOT)).resolve()
    datapack_dir = resolve_datapack_dir(HARNESS_ROOT, args.datapack_dir)
    benchmark_pack = load_benchmark_pack(fixture_dir)
    oracles = load_personas_oracle(fixture_dir)
    if args.personas:
        persona_list = [p.strip() for p in args.personas.split(",") if p.strip()]
    else:
        persona_list = sorted(oracles.keys())

    benchmarks = benchmark_pack.benchmarks
    for persona in persona_list:
        if persona not in benchmarks:
            raise SystemExit(f"Unknown persona: {persona}")

    cache_phases = [p.strip() for p in args.cache_phases.split(",") if p.strip()]
    if args.parallel_personas > 1 and "cold" in cache_phases:
        log("WARN: cold cache phase with parallel personas may be noisy")

    set_persona_passwords: Optional[bool] = None
    if args.set_persona_passwords:
        set_persona_passwords = True
    elif args.skip_set_persona_passwords:
        set_persona_passwords = False

    return SharedRunArgs(
        fixture_dir=fixture_dir,
        datapack_dir=datapack_dir,
        persona_list=persona_list,
        warmup=args.warmup,
        iterations=args.iterations,
        cache_phases=cache_phases,
        parallel_personas=args.parallel_personas,
        full_correctness=args.full_correctness,
        es_jitter=args.es_jitter,
        force_reload=args.force_reload,
        skip_load=args.skip_load,
        allow_downgrade=args.allow_downgrade,
        username=args.username,
        password=args.password,
        set_persona_passwords=set_persona_passwords,
        repo_root=HARNESS_ROOT.parent.parent,
    )


def run_single_target(
    target: TargetSpec,
    variant: RunVariant,
    shared: SharedRunArgs,
    output_path: Path,
    *,
    orchestration_id: Optional[str] = None,
    system_info: Optional[SystemInfoSnapshot] = None,
) -> None:
    frontend_url = resolve_frontend_url(target)
    creds = resolve_credentials(
        gms_url=target.gms_url,
        frontend_url=frontend_url,
        username=target.username or shared.username,
        password=target.password or shared.password,
        token=target.token,
        allow_prompt=credentials_allow_prompt(),
    )
    gms_url = creds.gms_url
    frontend_url = creds.frontend_url
    token = creds.token

    gms_config = fetch_gms_config(gms_url, token)
    if system_info is None:
        system_info = fetch_system_info(gms_url, token)
    target_version = read_pack_index_version(shared.datapack_dir)

    ensure_datapack(
        gms_url=gms_url,
        datapack_dir=shared.datapack_dir,
        target_version=target_version,
        token=token,
        force_reload=shared.force_reload,
        skip_load=shared.skip_load,
        allow_downgrade=shared.allow_downgrade,
        gms_version=gms_config.gms_version,
        gms_commit=gms_config.gms_commit,
    )

    if should_set_persona_passwords(
        gms_url,
        env_file=target.env_file,
        explicit=shared.set_persona_passwords,
    ):
        ensure_persona_passwords(
            frontend_url=frontend_url,
            admin_token=token,
            gms_url=gms_url,
            personas=shared.persona_list,
        )

    oracles = load_personas_oracle(shared.fixture_dir)
    benchmark_pack = load_benchmark_pack(shared.fixture_dir)
    benchmarks = adapt_benchmarks_for_view_auth(
        benchmark_pack.benchmarks,
        view_authorization_enabled=system_info.view_authorization_enabled,
    )
    git = capture_git_metadata(shared.repo_root)
    deployment = capture_deployment_metadata(
        gms_url,
        frontend_url,
        gms_host=gms_config.gms_host,
        gms_version=gms_config.gms_version,
        gms_commit=gms_config.gms_commit,
        server_type=gms_config.server_type,
        gms_config_error=gms_config.error,
        target_name=target.name,
        env_file=target.env_file,
        docker_tag=variant.docker_tag,
        view_authorization_enabled=system_info.view_authorization_enabled,
        system_info_error=system_info.error,
        system_info_properties=system_info.properties or None,
    )
    fixture = capture_fixture_metadata(
        shared.fixture_dir,
        datapack_dir=shared.datapack_dir,
        pack_index_version=target_version,
    )
    metadata = metadata_dict(
        git,
        deployment,
        fixture,
        run_label=variant.run_label,
        orchestration_id=orchestration_id,
    )

    run_id = new_run_id()
    graphql_registry = setup_graphql_queries(gms_url, token, benchmark_pack)
    config = RunConfig(
        run_id=run_id,
        metadata=metadata,
        gms_url=gms_url,
        frontend_url=frontend_url,
        warmup=shared.warmup,
        iterations=shared.iterations,
        cache_phases=shared.cache_phases,
        parallel_personas=shared.parallel_personas,
        full_correctness=shared.full_correctness,
        es_jitter=shared.es_jitter,
        graphql_registry=graphql_registry,
        view_authorization_enabled=system_info.view_authorization_enabled,
    )

    writer = JsonlWriter(output_path.resolve())
    try:
        rows = run_all_personas(
            shared.persona_list, benchmarks, oracles, config, writer
        )
    finally:
        writer.close()

    summary = write_summary(output_path.resolve(), rows)
    log(f"results appended to {output_path}")
    log(f"summary written to {summary}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    _validate_cli(args)

    multi_mode = args.output_dir is not None
    targets = parse_targets_from_args(
        env_files=args.env_file,
        env_file_names=args.env_file_name,
        target_strings=args.target,
        gms_url=args.gms_url,
        frontend_url=args.frontend_url,
    )
    if not targets:
        creds = resolve_credentials(
            gms_url=args.gms_url,
            frontend_url=args.frontend_url,
            username=args.username,
            password=args.password,
            allow_prompt=credentials_allow_prompt(),
        )
        targets = [
            TargetSpec(
                name="local",
                gms_url=creds.gms_url,
                frontend_url=creds.frontend_url,
            )
        ]
    validate_target_list(targets, multi_mode=multi_mode)

    variants = parse_variants(args.run_label, args.docker_tag)
    shared = _build_shared_args(args)
    git_preview = capture_git_metadata(shared.repo_root)
    matrix_size = variant_matrix_size(len(targets), len(variants))
    multi_cell = matrix_size > 1

    if not multi_mode:
        target = targets[0]
        variant = variants[0]
        run_single_target(target, variant, shared, args.output)  # type: ignore[arg-type]
        return 0

    orchestration_id = new_run_id()
    output_dir = args.output_dir.resolve()  # type: ignore[union-attr]
    manifest_entries: list[ManifestRunEntry] = []
    exit_code = 0

    auth_snapshots: dict[str, SystemInfoSnapshot] = {}
    if len(targets) > 1:
        for target in targets:
            creds = resolve_credentials(
                gms_url=target.gms_url,
                frontend_url=resolve_frontend_url(target),
                username=target.username or shared.username,
                password=target.password or shared.password,
                token=target.token,
                allow_prompt=False,
            )
            auth_snapshots[target.name] = fetch_system_info(
                creds.gms_url,
                creds.token,
            )
        warn_authorization_settings_drift(auth_snapshots)

    for target in targets:
        for variant in variants:
            gms_config = fetch_gms_config(
                target.gms_url,
                target.token,
            )
            slug = build_slug(variant, git_preview, gms_config.gms_version)
            jsonl_path, summary_path = result_paths(
                output_dir,
                target.name,
                slug,
                orchestration_id,
                multi_cell=multi_cell,
            )
            log(
                f"running target={target.name} variant={slug} -> {jsonl_path.name}"
            )
            try:
                target_auth = auth_snapshots.get(target.name)
                run_single_target(
                    target,
                    variant,
                    shared,
                    jsonl_path,
                    orchestration_id=orchestration_id,
                    system_info=target_auth,
                )
                manifest_entries.append(
                    ManifestRunEntry(
                        target_name=target.name,
                        build_slug=slug,
                        gms_url=target.gms_url,
                        gms_host=gms_config.gms_host,
                        gms_version=gms_config.gms_version,
                        gms_commit=gms_config.gms_commit,
                        git=git_dict(git_preview),
                        output_jsonl=str(jsonl_path),
                        output_summary=str(summary_path),
                        status="ok",
                        view_authorization_enabled=(
                            target_auth.view_authorization_enabled
                            if target_auth is not None
                            else None
                        ),
                    )
                )
            except Exception as exc:
                log(f"ERROR: target={target.name} variant={slug}: {exc}")
                target_auth = auth_snapshots.get(target.name)
                manifest_entries.append(
                    ManifestRunEntry(
                        target_name=target.name,
                        build_slug=slug,
                        gms_url=target.gms_url,
                        gms_host=gms_config.gms_host,
                        gms_version=gms_config.gms_version,
                        gms_commit=gms_config.gms_commit,
                        git=git_dict(git_preview),
                        output_jsonl=str(jsonl_path),
                        output_summary=str(summary_path),
                        status="error",
                        error=str(exc),
                        view_authorization_enabled=(
                            target_auth.view_authorization_enabled
                            if target_auth is not None
                            else None
                        ),
                    )
                )
                exit_code = 1
                if args.fail_fast:
                    break
        if args.fail_fast and exit_code != 0:
            break

    fixture_version = read_pack_index_version(shared.datapack_dir)
    manifest_path = write_manifest(
        output_dir,
        orchestration_id=orchestration_id,
        harness_args={
            "warmup": shared.warmup,
            "iterations": shared.iterations,
            "cache_phases": shared.cache_phases,
            "parallel_personas": shared.parallel_personas,
            "personas": shared.persona_list,
        },
        fixture_version=fixture_version,
        runs=manifest_entries,
    )
    log(f"manifest written to {manifest_path}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
