"""Full local extraction for Elysium using the shared plant-map pipeline."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "consumerScripts"))

from map_plants_common import (  # noqa: E402
    build_foliage_group_map,
    collect_package_refs,
    extract_world_positions,
    find_data_root,
    load_world_configs,
    run_ue4export,
    serialize_positions_by_world,
    write_asset_list,
)

UE4EXPORT = Path(__file__).resolve().parent / "Ue4Export" / "Ue4Export.exe"
PAKS_DIR = Path(r"D:\Games\Steam\steamapps\common\Icarus\Icarus\Content\Paks")
TARGET_PAKS = [
    "pakchunk0_s30-WindowsNoEditor.pak",
    "pakchunk0_s31-WindowsNoEditor.pak",
]
WORLD_ID = "Terrain_021"


def stage_target_paks(staging_dir: Path) -> None:
    staging_dir.mkdir(parents=True, exist_ok=True)
    for pak_name in TARGET_PAKS:
        pak_path = PAKS_DIR / pak_name
        shutil.copy2(pak_path, staging_dir / pak_path.name)
        sig_path = pak_path.with_suffix(".pak.sig")
        if sig_path.exists():
            shutil.copy2(sig_path, staging_dir / sig_path.name)


def main() -> None:
    data_root = find_data_root()
    worlds = load_world_configs(data_root, [WORLD_ID])
    if not worlds:
        raise SystemExit(f"World {WORLD_ID} not found.")

    foliage_to_group, resource_actor_by_foliage = build_foliage_group_map(data_root)
    package_refs = collect_package_refs(worlds)

    with tempfile.TemporaryDirectory(prefix="icarus-elysium-full-") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        staged_paks_dir = tmp_dir / "paks"
        text_dir = tmp_dir / "text"
        raw_dir = tmp_dir / "raw"
        text_list = tmp_dir / "assets_text.txt"
        raw_list = tmp_dir / "assets_raw.txt"

        stage_target_paks(staged_paks_dir)
        write_asset_list(text_list, "Text", package_refs)
        write_asset_list(raw_list, "Raw", package_refs)

        print("Exporting text packages...")
        run_ue4export(UE4EXPORT, staged_paks_dir, text_list, text_dir)
        print("Exporting raw packages...")
        run_ue4export(UE4EXPORT, staged_paks_dir, raw_list, raw_dir)

        positions_by_world, unknown_by_world, processed_counts = extract_world_positions(
            worlds,
            text_dir,
            raw_dir,
            foliage_to_group,
            resource_actor_by_foliage,
        )

    serialized = serialize_positions_by_world(positions_by_world)
    elysium_positions = serialized.get(WORLD_ID, {})
    total = sum(len(group_positions) for group_positions in elysium_positions.values())

    print(f"Processed {processed_counts[WORLD_ID]} packages for {WORLD_ID}")
    for group_id in sorted(elysium_positions):
        print(f"  {group_id}: {len(elysium_positions[group_id])} instances")
    print(f"\n  Total: {total} instances across {len(elysium_positions)} plant types")

    if unknown_by_world[WORLD_ID]:
        print("\nUnmapped plant-like foliage:")
        for entry, count in unknown_by_world[WORLD_ID].most_common():
            print(f"  {entry}: {count}")

    repo_root = Path(__file__).resolve().parents[1]
    out_path = repo_root / "tmp_raw_positions_new.json"
    with out_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(elysium_positions, handle, separators=(",", ":"), ensure_ascii=False)
        handle.write("\n")
    print(f"\nSaved to {out_path}")

    old_path = repo_root / "tmp_raw_positions.json"
    if old_path.exists():
        with old_path.open("r", encoding="utf-8") as handle:
            old = json.load(handle)
        print("\nComparison with previous extraction:")
        all_groups = sorted(set(old) | set(elysium_positions))
        for group_id in all_groups:
            old_count = len(old.get(group_id, []))
            new_count = len(elysium_positions.get(group_id, []))
            diff = new_count - old_count
            marker = " <-- DIFF" if diff != 0 else ""
            sign = "+" if diff > 0 else ""
            print(f"  {group_id}: {old_count} -> {new_count} ({sign}{diff}){marker}")


if __name__ == "__main__":
    main()
