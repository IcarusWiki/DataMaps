#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import shutil
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

from map_plants_common import (
    DEFAULT_MAX_CLUSTER_COUNT,
    STATIC_MESH_GROUPS,
    WorldConfig,
    build_datamaps_json,
    build_foliage_group_map,
    cluster_positions,
    dedupe_exact_positions,
    exported_json_path,
    exported_uexp_path,
    find_data_root,
    load_world_configs,
    process_sublevel,
    run_ue4export,
    write_asset_list,
    write_datamaps_json,
)

DEFAULT_PAKS_DIR = r"D:\Games\Steam\steamapps\common\Icarus\Icarus\Content\Paks"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build DataMaps plant JSON files for all worlds and outposts.",
    )
    parser.add_argument(
        "--data-root",
        help="Path to the sibling Data repo (defaults to autodetect).",
    )
    parser.add_argument(
        "--paks-dir",
        default=DEFAULT_PAKS_DIR,
        help=f"Icarus pak directory (default: {DEFAULT_PAKS_DIR})",
    )
    parser.add_argument(
        "--out-dir",
        default=str(Path(__file__).resolve().parents[1] / "wiki" / "maps"),
        help="Output directory for *Plants.json files.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Cluster diameter in world units. Omit to use the adaptive default.",
    )
    parser.add_argument(
        "--max-cluster-count",
        type=int,
        default=DEFAULT_MAX_CLUSTER_COUNT,
        help=f"Maximum items allowed in a single cluster (default: {DEFAULT_MAX_CLUSTER_COUNT})",
    )
    parser.add_argument(
        "--pak-min",
        type=int,
        default=21,
        help="Minimum pak season suffix to include (default: 21).",
    )
    parser.add_argument(
        "--pak-max",
        type=int,
        default=31,
        help="Maximum pak season suffix to include (default: 31).",
    )
    parser.add_argument(
        "--world",
        action="append",
        dest="worlds",
        help="Optional world ID filter, repeatable (e.g. Terrain_021).",
    )
    return parser.parse_args()


def select_paks(paks_dir: Path, pak_min: int, pak_max: int) -> list[Path]:
    pattern = re.compile(r"_s(\d+)-WindowsNoEditor\.pak$", re.IGNORECASE)
    selected: list[Path] = []
    for pak_path in sorted(paks_dir.glob("*.pak")):
        match = pattern.search(pak_path.name)
        if not match:
            continue
        suffix = int(match.group(1))
        if pak_min <= suffix <= pak_max:
            selected.append(pak_path)
    return selected


def stage_paks(pak_paths: list[Path], staging_dir: Path) -> None:
    staging_dir.mkdir(parents=True, exist_ok=True)
    for pak_path in pak_paths:
        shutil.copy2(pak_path, staging_dir / pak_path.name)
        sig_path = pak_path.with_suffix(".pak.sig")
        if sig_path.exists():
            shutil.copy2(sig_path, staging_dir / sig_path.name)


def collect_package_refs(worlds: list[WorldConfig]) -> list[str]:
    package_refs: list[str] = []
    for world in worlds:
        for package_ref in world.package_refs:
            if package_ref not in package_refs:
                package_refs.append(package_ref)
    return package_refs


def main() -> None:
    args = parse_args()

    data_root = find_data_root(args.data_root)
    worlds = load_world_configs(data_root, args.worlds)
    if not worlds:
        raise SystemExit("No matching worlds found.")

    paks_dir = Path(args.paks_dir)
    pak_paths = select_paks(paks_dir, args.pak_min, args.pak_max)
    if not pak_paths:
        raise SystemExit(f"No pak files matched s{args.pak_min}..s{args.pak_max} in {paks_dir}")

    foliage_to_group, resource_actor_by_foliage = build_foliage_group_map(data_root)
    print(f"Loaded {len(worlds)} worlds and {len(foliage_to_group)} foliage aliases")
    print(f"Using {len(pak_paths)} pak files from {paks_dir}")

    out_dir = Path(args.out_dir)
    output_paths = []

    with tempfile.TemporaryDirectory(prefix="icarus-map-plants-") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        staged_paks_dir = tmp_dir / "paks"
        text_dir = tmp_dir / "text"
        raw_dir = tmp_dir / "raw"
        text_list = tmp_dir / "assets_text.txt"
        raw_list = tmp_dir / "assets_raw.txt"

        print("Staging pak files...")
        stage_paks(pak_paths, staged_paks_dir)

        package_refs = collect_package_refs(worlds)
        write_asset_list(text_list, "Text", package_refs)
        write_asset_list(raw_list, "Raw", package_refs)

        ue4export_exe = Path(__file__).resolve().parents[1] / "tools" / "Ue4Export" / "Ue4Export.exe"
        print("Exporting text packages...")
        run_ue4export(str(ue4export_exe), staged_paks_dir, text_list, text_dir)
        print("Exporting raw packages...")
        run_ue4export(str(ue4export_exe), staged_paks_dir, raw_list, raw_dir)

        positions_by_world: dict[str, dict[str, list[tuple[float, float, float]]]] = {
            world.world_id: defaultdict(list) for world in worlds
        }
        unknown_by_world: dict[str, Counter[str]] = {world.world_id: Counter() for world in worlds}

        for world in worlds:
            print(f"\n{world.display_name}")
            processed_levels = 0
            for package_ref in world.package_refs:
                json_path = exported_json_path(text_dir, package_ref)
                uexp_path = exported_uexp_path(raw_dir, package_ref)
                if not json_path.is_file() or not uexp_path.is_file():
                    continue
                level_positions, unknown = process_sublevel(
                    json_path,
                    uexp_path,
                    foliage_to_group,
                    resource_actor_by_foliage,
                )
                processed_levels += 1
                for group_id, group_positions in level_positions.items():
                    positions_by_world[world.world_id][group_id].extend(group_positions)
                unknown_by_world[world.world_id].update(unknown)

            for group_id in set(STATIC_MESH_GROUPS.values()):
                if group_id in positions_by_world[world.world_id]:
                    positions_by_world[world.world_id][group_id] = dedupe_exact_positions(
                        positions_by_world[world.world_id][group_id]
                    )

            raw_count = sum(len(group_positions) for group_positions in positions_by_world[world.world_id].values())
            print(f"  Processed {processed_levels} packages and found {raw_count} raw instances")
            if unknown_by_world[world.world_id]:
                print(f"  Unmapped plant-like foliage: {len(unknown_by_world[world.world_id])}")

        for world in worlds:
            grouped_positions = positions_by_world[world.world_id]
            clustered: dict[str, list[tuple[float, float, int]]] = {}
            min_x, min_y, max_x, max_y = world.bounds
            for group_id in sorted(grouped_positions):
                clusters = cluster_positions(
                    grouped_positions[group_id],
                    min_x,
                    min_y,
                    max_x,
                    max_y,
                    threshold=args.threshold,
                    max_cluster_count=args.max_cluster_count,
                )
                if clusters:
                    clustered[group_id] = clusters

            output_json = build_datamaps_json(world, clustered)
            output_path = out_dir / f"{world.output_stem}Plants.json"
            write_datamaps_json(output_path, output_json)
            output_paths.append(output_path)

            raw_count = sum(len(group_positions) for group_positions in grouped_positions.values())
            cluster_count = sum(len(clusters) for clusters in clustered.values())
            print(
                f"\nWrote {output_path.name}: "
                f"{cluster_count} markers from {raw_count} instances across {len(clustered)} plant types"
            )
            if unknown_by_world[world.world_id]:
                for entry, count in unknown_by_world[world.world_id].most_common():
                    print(f"  skipped {entry}: {count}")

    print("\nGenerated files:")
    for output_path in output_paths:
        print(f"  {output_path}")


if __name__ == "__main__":
    main()
