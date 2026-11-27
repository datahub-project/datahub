"""Inventory commands layered on the ownership-checked local lifecycle."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from types import ModuleType
from typing import cast

import datahub_dev_local as local


def inventory(dev: ModuleType) -> list[dict[str, object]]:
    entries = dev._load_registry()["instances"]
    ids = set(entries)
    if local.LOCAL_DATA_ROOT.exists():
        ids.update(
            path.name for path in local.LOCAL_DATA_ROOT.iterdir() if path.is_dir()
        )
    rows: list[dict[str, object]] = []
    for stack_id in sorted(ids):
        if not re.fullmatch(r"[0-9a-f]{6}", stack_id):
            continue
        record = local.read_instance_record(stack_id)
        entry = entries.get(stack_id)
        if record is None or (
            entry is not None and (not local.is_owned_entry(entry) or record != entry)
        ):
            continue
        ports = cast(dict[str, int], record["ports"])
        path = str(record["worktree_path"])
        running = local.starting_alive(record) or local.stack_running(stack_id, ports)
        state = "running" if running else "stopped"
        if entry is None:
            state = "unregistered"
        elif not Path(path).exists() and not running:
            state = "stale"
        status = local.opensearch_manager(local.recorded_major(record)).status()
        shards = 0
        indices = 0
        if status["healthy"]:
            _, counts = local.shard_inventory(int(str(status["port"])))
            prefixes = local._prefixes(stack_id)
            selected = [
                count
                for name, count in counts.items()
                if any(name.startswith(prefix + "_") for prefix in prefixes)
            ]
            indices, shards = len(selected), sum(selected)
        rows.append(
            {
                "id": stack_id,
                "state": state,
                "running": running,
                "worktree_path": path,
                "record": record,
                "indices": indices,
                "shards": shards,
            }
        )
    return rows


def run(args: argparse.Namespace, dev: ModuleType) -> int:
    rows = inventory(dev)
    prune = args.command == "indices" and args.indices_command == "prune"
    include_orphans = prune or getattr(args, "orphans", False)
    clean = prune or (args.command == "instances" and args.instances_command == "clean")
    if clean:
        for stack_id, record in dev._load_registry()["instances"].items():
            worktree = record.get("worktree_path")
            if (
                record.get("mode") != "local"
                and worktree
                and not Path(worktree).exists()
            ):
                rows.append(
                    {
                        "id": stack_id,
                        "state": "stale",
                        "record": record,
                        "running": False,
                        "worktree_path": worktree,
                    }
                )
    if args.command == "indices" and args.indices_command == "list":
        print(json.dumps({"stacks": rows}, indent=2))
        return 0
    repair = args.command == "instances" and args.instances_command == "repair"
    drop = args.command == "indices" and args.indices_command == "drop"
    selected = [
        row
        for row in rows
        if (
            row["state"] == "unregistered"
            if repair
            else row["id"] == args.id
            if drop
            else row["state"] == "stale"
            or (
                include_orphans
                and row["state"] == "unregistered"
                and not Path(str(row["worktree_path"])).exists()
            )
        )
    ]
    if drop and not selected:
        raise RuntimeError("No matching owned stack with an unchanged instance record")
    for row in selected:
        if not repair and row["running"]:
            raise RuntimeError(f"Stack {row['id']} is running. Stop it first")
        print(
            f"{'Applying' if args.yes else 'Dry run'}: {'repair' if repair else 'remove'} {row['id']}"
        )
        if not args.yes:
            continue
        stack_id = str(row["id"])
        with dev._registry_transaction() as registry:
            record = row["record"]
            if isinstance(record, dict) and record.get("mode") != "local":
                current_entry = registry["instances"].get(stack_id)
                orphan = (
                    include_orphans
                    and row["state"] == "unregistered"
                    and current_entry is None
                )
                if current_entry != record and not orphan:
                    raise RuntimeError("Registry changed; retry")
                del registry["instances"][stack_id]
                continue
            record = local.read_instance_record(stack_id)
            if record != row["record"]:
                raise RuntimeError("Instance record changed; retry")
            if repair:
                if stack_id in registry["instances"]:
                    raise RuntimeError("Instance was registered concurrently; retry")
                assert record is not None
                claimed = {
                    port
                    for entry in registry["instances"].values()
                    for port in entry.get("ports", {}).values()
                }
                claimed_slots = {
                    entry.get("slot") for entry in registry["instances"].values()
                }
                if record.get("slot") in claimed_slots or claimed.intersection(
                    cast(dict[str, int], record["ports"]).values()
                ):
                    raise RuntimeError("Cannot repair: ports are already reserved")
                registry["instances"][stack_id] = record
            else:
                current_entry = registry["instances"].get(stack_id)
                orphan = (
                    include_orphans
                    and row["state"] == "unregistered"
                    and current_entry is None
                )
                if current_entry != record and not orphan:
                    raise RuntimeError(
                        "Refusing cleanup without matching registry ownership; repair first"
                    )
                assert record is not None
                if local.starting_alive(record) or local.stack_running(
                    stack_id, cast(dict[str, int], record["ports"])
                ):
                    raise RuntimeError(f"Stack {stack_id} is running. Stop it first")
        if repair:
            continue
        # Search cleanup can start a node; leave reservations visible without holding the registry lock.
        assert record is not None
        if local.cmd_nuke_local(stack_id, cast(dict[str, int], record["ports"]), False):
            return 1
        with dev._registry_transaction() as registry:
            if registry["instances"].get(stack_id) != current_entry:
                continue
            if Path(str(record["worktree_path"])).exists():
                # Keep live worktrees startable after their data has been dropped.
                local.write_instance_record(stack_id, record)
            else:
                registry["instances"].pop(stack_id, None)
    return 0
