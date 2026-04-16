#!/usr/bin/env python3
"""
Merge per-pak raw plant-position partials and generate `plantMaps/*Plants.json`.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from map_plants_common import (
    DEFAULT_MAX_CLUSTER_COUNT,
    DEFAULT_OUTPUT_DIR,
    STATIC_MESH_GROUPS,
    build_datamaps_json,
    cluster_positions,
    dedupe_exact_positions,
    find_data_root,
    load_partial_positions,
    load_world_configs,
    write_datamaps_json,
)


def parse_args() -> argparse.Namespace:
    default_partials_dir = ""
    finalizer_artifact_dir = os.environ.get("ICARUS_FINALIZER_ARTIFACT_DIR", "")
    if finalizer_artifact_dir:
        default_partials_dir = str(Path(finalizer_artifact_dir) / "plant-map-partials")
    parser = argparse.ArgumentParser(
        description="Merge raw plant-map partials and generate final *Plants.json files.",
    )
    parser.add_argument(
        "--partials-dir",
        default=default_partials_dir,
        help="Directory containing per-pak partial JSON files.",
    )
    parser.add_argument(
        "--data-root",
        default=os.environ.get("ICARUS_DATA_REPO_DIR"),
        help="Path to the Data repo root (defaults to autodetect).",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for generated *Plants.json files.",
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
        help=(
            "Maximum items allowed in a single cluster "
            f"(default: {DEFAULT_MAX_CLUSTER_COUNT})."
        ),
    )
    parser.add_argument(
        "--world",
        action="append",
        dest="worlds",
        help="Optional world ID filter, repeatable (e.g. Terrain_021).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.partials_dir:
        raise SystemExit("--partials-dir is required (or set ICARUS_FINALIZER_ARTIFACT_DIR)")

    data_root = find_data_root(args.data_root)
    worlds = load_world_configs(data_root, args.worlds)
    if not worlds:
        raise SystemExit("No matching worlds found.")

    merged_positions = load_partial_positions(args.partials_dir)
    if not merged_positions:
        raise SystemExit("No partial raw-position files were found.")

    dedupe_group_ids = set(STATIC_MESH_GROUPS.values())
    out_dir = Path(args.out_dir)

    for world in worlds:
        grouped_positions = {
            group_id: list(positions)
            for group_id, positions in merged_positions.get(world.world_id, {}).items()
        }
        for group_id in dedupe_group_ids:
            if group_id in grouped_positions:
                grouped_positions[group_id] = dedupe_exact_positions(grouped_positions[group_id])

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

        output_path = out_dir / f"{world.output_stem}Plants.json"
        write_datamaps_json(output_path, build_datamaps_json(world, clustered))

        raw_count = sum(len(group_positions) for group_positions in grouped_positions.values())
        marker_count = sum(len(clusters) for clusters in clustered.values())
        print(
            f"Wrote {output_path.name}: {marker_count} markers from "
            f"{raw_count} instances across {len(clustered)} plant types"
        )


if __name__ == "__main__":
    main()
