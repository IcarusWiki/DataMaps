#!/usr/bin/env python3
"""
Processor script for extracting raw plant positions from a single pak.

Runs inside the Data workflow's per-pak processor environment. For the current
pak, it exports all known map packages that are present, extracts raw plant
positions for every supported world/outpost, and writes one partial JSON file
that is later merged into the final `plantMaps/*Plants.json` outputs.

Environment variables (set by process_single_pak.py):
  ICARUS_PAK_FILE       - Path to the original .pak file
  ICARUS_PAK_UNPACK_DIR - Directory containing the pak's extracted files
  ICARUS_PAK_WORK_ROOT  - Per-pak temp work directory
  ICARUS_PAK_ARTIFACT_ROOT - Directory for this processor's persisted outputs
  ICARUS_CONSUMER_ID    - Consumer id from the Data workflow config
  ICARUS_PROCESSOR_ARTIFACT_ID - Artifact id from the Data workflow config
  ICARUS_PAK_NAME       - Friendly pak name
  UE4EXPORT_EXE         - Path to Ue4Export.exe
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

from map_plants_common import (
    build_foliage_group_map,
    collect_package_refs,
    extract_world_positions,
    filter_present_package_refs,
    find_data_root,
    load_world_configs,
    run_ue4export,
    serialize_positions_by_world,
    write_asset_list,
)


def fail(message: str) -> None:
    print(f"Error: {message}")
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract raw plant-map positions from the current pak.",
    )
    parser.add_argument(
        "--data-root",
        help="Path to the Data repo root (defaults to autodetect / GITHUB_WORKSPACE).",
    )
    parser.add_argument(
        "--partial-dir",
        help="Directory where per-pak partial JSON files should be written.",
    )
    parser.add_argument(
        "--world",
        action="append",
        dest="worlds",
        help="Optional world ID filter, repeatable (e.g. Terrain_021).",
    )
    return parser.parse_args()


def resolve_partial_dir(args: argparse.Namespace, work_root: Path) -> Path:
    configured = (
        args.partial_dir
        or os.environ.get("ICARUS_PAK_ARTIFACT_ROOT")
        or os.path.join(os.environ.get("RUNNER_TEMP", str(work_root.parent)), "consumer-artifacts")
    )
    path = Path(configured)
    path.mkdir(parents=True, exist_ok=True)
    return path


def main() -> None:
    args = parse_args()

    pak_file = os.environ.get("ICARUS_PAK_FILE", "")
    unpack_dir = os.environ.get("ICARUS_PAK_UNPACK_DIR", "")
    work_root = os.environ.get("ICARUS_PAK_WORK_ROOT", "")
    pak_name = os.environ.get("ICARUS_PAK_NAME", "unknown")
    ue4export_exe = os.environ.get("UE4EXPORT_EXE", "")
    consumer_id = os.environ.get("ICARUS_CONSUMER_ID", "")
    artifact_id = os.environ.get("ICARUS_PROCESSOR_ARTIFACT_ID", "")

    if not pak_file or not work_root:
        fail("ICARUS_PAK_FILE and ICARUS_PAK_WORK_ROOT must be set")
    if not ue4export_exe:
        fail("UE4EXPORT_EXE must be set")

    pak_path = Path(pak_file)
    if not pak_path.is_file():
        fail(f"Pak file not found: {pak_path}")

    data_root = find_data_root(args.data_root)
    worlds = load_world_configs(data_root, args.worlds)
    if not worlds:
        fail("No matching worlds found.")

    foliage_to_group, resource_actor_by_foliage = build_foliage_group_map(data_root)
    package_refs = collect_package_refs(worlds)
    if unpack_dir:
        unpack_path = Path(unpack_dir)
        if not unpack_path.is_dir():
            fail(f"ICARUS_PAK_UNPACK_DIR does not exist: {unpack_path}")
        all_package_refs = len(package_refs)
        package_refs = filter_present_package_refs(unpack_path, package_refs)
        print(
            f"[plant-maps] {pak_name}: "
            f"{len(package_refs)}/{all_package_refs} known map packages present in this pak"
        )
    else:
        print(
            "[plant-maps] ICARUS_PAK_UNPACK_DIR is not set; "
            "exporting all known map packages for the selected worlds"
        )

    if not package_refs:
        print(f"[plant-maps] No supported map packages found in {pak_name}, skipping.")
        return

    partial_dir = resolve_partial_dir(args, Path(work_root))

    with tempfile.TemporaryDirectory(prefix="icarus-plant-map-", dir=work_root) as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        text_dir = tmp_dir / "text"
        raw_dir = tmp_dir / "raw"
        text_list = tmp_dir / "assets_text.txt"
        raw_list = tmp_dir / "assets_raw.txt"

        write_asset_list(text_list, "Text", package_refs)
        write_asset_list(raw_list, "Raw", package_refs)

        print(f"[plant-maps] Exporting package refs from {pak_name}...")
        run_ue4export(ue4export_exe, pak_path.parent, text_list, text_dir)
        run_ue4export(ue4export_exe, pak_path.parent, raw_list, raw_dir)

        positions_by_world, unknown_by_world, processed_counts = extract_world_positions(
            worlds,
            text_dir,
            raw_dir,
            foliage_to_group,
            resource_actor_by_foliage,
        )

    serialized = serialize_positions_by_world(positions_by_world)
    total_instances = sum(
        len(group_positions)
        for grouped_positions in serialized.values()
        for group_positions in grouped_positions.values()
    )

    for world in worlds:
        grouped_positions = serialized.get(world.world_id, {})
        raw_count = sum(len(group_positions) for group_positions in grouped_positions.values())
        if processed_counts[world.world_id] == 0 and raw_count == 0:
            continue
        print(
            f"[plant-maps] {world.display_name}: "
            f"{processed_counts[world.world_id]} packages, {raw_count} raw instances"
        )
        if unknown_by_world[world.world_id]:
            print(
                f"[plant-maps] {world.display_name}: "
                f"{len(unknown_by_world[world.world_id])} unmapped plant-like foliage aliases"
            )

    if total_instances == 0:
        print(f"[plant-maps] No supported plant instances found in {pak_name}, skipping.")
        return

    payload = {"worlds": serialized}
    safe_name = pak_name.replace(" ", "_").replace("/", "_")
    partial_path = partial_dir / f"plant-maps-{safe_name}.json"
    with partial_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, separators=(",", ":"), ensure_ascii=False)
        handle.write("\n")

    print(
        f"[plant-maps] Wrote {total_instances} raw instances across "
        f"{len(serialized)} worlds to {partial_path} "
        f"(consumer={consumer_id or 'n/a'}, artifact={artifact_id or 'n/a'})"
    )


if __name__ == "__main__":
    main()
